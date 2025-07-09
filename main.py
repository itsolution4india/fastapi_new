import logging
import os
import uvicorn
from fastapi import HTTPException, Query
from fastapi import BackgroundTasks
from fastapi import File, UploadFile, Form
from typing import List, Optional
from models import APIMessageRequest, APIBalanceRequest, ValidateNumbers, MessageRequest, BotMessageRequest, CarouselRequest, FlowMessageRequest, ReportRequest
from utils import logger, generate_unique_id
from async_api_functions import fetch_user_data, validate_coins, update_balance_and_report, get_template_details_by_name, generate_media_id
from async_chunk_functions import send_messages, send_carousels, send_bot_messages, send_template_with_flows, validate_numbers_async
from app import app, load_tracker
import httpx

import mysql.connector

TEMP_FOLDER = "temp_uploads"
os.makedirs(TEMP_FOLDER, exist_ok=True)

SECONDARY_SERVER = "http://fastapi2.wtsmessage.xyz"
IS_PRIMARY = False
        

@app.post("/send_sms/")
async def send_messages_api(request: MessageRequest, background_tasks: BackgroundTasks):
    try:
        unique_id = generate_unique_id()
        
        if IS_PRIMARY and load_tracker.is_busy():
            logger.info(f"Server busy, redirecting request {unique_id} to secondary server")
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{SECONDARY_SERVER}/send_sms/",
                        json=request.dict(),
                        timeout=10.0
                    )
                    if response.status_code == 200:
                        return response.json()
                    else:
                        logger.error(f"Secondary server error: {response.text}")
                        raise HTTPException(status_code=503, detail="All servers busy")
            except Exception as e:
                logger.error(f"Error redirecting to secondary server: {e}")
                raise HTTPException(status_code=503, detail="Error redirecting request")
        
        
        background_tasks.add_task(
            send_messages,
            token=request.token,
            phone_number_id=request.phone_number_id,
            template_name=request.template_name,
            language=request.language,
            media_type=request.media_type,
            media_id=request.media_id,
            contact_list=request.contact_list,
            variable_list=request.variable_list,
            csv_variables=request.csv_variables,
            request_id=request.request_id,
            unique_id=unique_id,
            test_numbers=request.test_numbers
        )
        return {
            'message': 'Messages sent successfully',
            "unique_id": unique_id,
            "request_id": request.request_id
        }
    except HTTPException as e:
        logger.error(f"HTTP error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")
   
from fastapi.responses import StreamingResponse
from db_models import SessionLocal, ReportInfo
import pymysql
import csv
import io
 
dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "prashanth@itsolution4india.com",
    "password": "Solution@97",
    "db": "webhook_responses",
    "charset": "utf8mb4",
}
 
@app.post("/get_report/")
async def get_report(request: ReportRequest):
    try:
        # Step 1: Get contact_list and waba_id_list from PostgreSQL
        db = SessionLocal()
        report = db.query(ReportInfo).filter(ReportInfo.id == int(request.report_id)).first()
        if not report:
            logger.warning(f"Report not found for ID {request.report_id}")
            return {"error": "Report not found"}

        contact_list = [x.strip() for x in report.contact_list.split(",") if x.strip()]
        waba_id_list = [x.strip() for x in report.waba_id_list.split(",") if x.strip()]
        db.close()

        # Step 2: Fetch matching rows from MySQL
        connection = pymysql.connect(**dbconfig)
        cursor = connection.cursor()

        if not contact_list or not waba_id_list:
            logger.warning(f"Empty contact or waba_id list for report ID {request.report_id}")
            return {"error": "Contact list or WABA ID list is empty"}

        placeholders_phones = ','.join(['%s'] * len(contact_list))
        placeholders_wabas = ','.join(['%s'] * len(waba_id_list))

        query = f"""
            SELECT 
                Date, display_phone_number, phone_number_id, waba_id, contact_wa_id,
                status, message_timestamp, error_code, error_message, contact_name,
                message_from, message_type, message_body
            FROM webhook_responses_786158633633821_dup
            WHERE phone_number_id IN ({placeholders_phones})
            AND waba_id IN ({placeholders_wabas})
        """
        cursor.execute(query, contact_list + waba_id_list)
        rows = cursor.fetchall()

        header = [
            "Date", "display_phone_number", "phone_number_id", "waba_id", "contact_wa_id",
            "status", "message_timestamp", "error_code", "error_message", "contact_name",
            "message_from", "message_type", "message_body"
        ]

        # Step 3: Write to CSV in-memory
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)

        output.seek(0)
        connection.close()

        logger.info(f"Successfully generated CSV for report ID {request.report_id}")

        # Step 4: Return as downloadable response
        filename = f"report_{request.report_id}.csv"
        return StreamingResponse(output, media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })

    except Exception as e:
        logger.error(f"Exception occurred during report generation: {e}", exc_info=True)
        return {"error": "Internal server error"}
    
@app.post("/send_carousel_messages/")
async def send_carousel_api(request: CarouselRequest, background_tasks: BackgroundTasks):
    try:
        unique_id = generate_unique_id()
        
        background_tasks.add_task(
            send_carousels,
            token=request.token,
            phone_number_id=request.phone_number_id,
            template_name=request.template_name,
            contact_list=request.contact_list,
            media_id_list= request.media_id_list,
            template_details = request.template_details,
            request_id=request.request_id,
            unique_id=unique_id
            
        )
        return {
            'message': 'Carousel Messages sent successfully',
            "unique_id": unique_id,
            "request_id": request.request_id
        }
    except HTTPException as e:
        logger.error(f"HTTP error: {e}")
        return str(e)
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        return str(e)

@app.post("/bot_api/")
async def bot_api(request: BotMessageRequest):
    logging.info(f"request {request}")
    try:
        await send_bot_messages(
            token=request.token,
            phone_number_id=request.phone_number_id,
            contact_list=request.contact_list,
            message_type=request.message_type,
            header=request.header,
            body=request.body,
            footer=request.footer,
            button_data=request.button_data,
            product_data=request.product_data,
            catalog_id=request.catalog_id,
            sections=request.sections,
            latitude=request.latitude,
            longitude=request.longitude,
            media_id=request.media_id
        )
        return {'message': 'Messages sent successfully'}
    except HTTPException as e:
        logger.error(f"HTTP error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

@app.post("/send_flow_message/")
async def send_flow_message_api(request: FlowMessageRequest, background_tasks: BackgroundTasks):
    try:
        unique_id = generate_unique_id()
        
        background_tasks.add_task(
            send_template_with_flows,
            request.token,
            request.phone_number_id,
            request.template_name,
            request.flow_id,
            request.language,
            request.recipient_phone_number,
            request.request_id,
            unique_id
        )
        return {
            'message': 'Flow message sent successfully',
            "unique_id": unique_id,
            "request_id": request.request_id
        }
    except Exception as e:
        logger.error(f"Unhandled error in send_flow_message_api: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

@app.get("/")
def root():
    logger.info("Root endpoint accessed.")
    return {"message": "Successful"}


@app.post("/send_sms_api/")
async def send_sms_post(request: APIMessageRequest):
    try:
        user_data = await fetch_user_data(request.user_id, request.api_token)
        logger.info(f"User validation successful for user_id: {request.user_id}")
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "510", "status": "failed", "detail": e.detail}

    try:
        template = await get_template_details_by_name(user_data.register_app__token, user_data.whatsapp_business_account_id, request.template_name)
        category = template.get('category', 'Category not found')
    except HTTPException as e:
        logger.error(f"Template validation Failed: {e.detail}")
        return {"error code": "404 Not Found", "status": "failed", "detail": e.detail}

    try:
        total_contacts = len(request.contact_list)
        if category == "AUTHENTICATION" or category == "UTILITY":
            user_coins = user_data.authentication_coins
        elif category == "MARKETING":
            user_coins = user_data.marketing_coins
        else:
            user_coins = user_data.authentication_coins + user_data.marketing_coins

        await validate_coins(user_coins, total_contacts)
        logger.info(f"Coin validation successful. Required: {total_contacts}, Available: {user_data.coins}")
    except HTTPException as e:
        logger.error(f"Coin validation failed: {e.detail}")
        return {"error code": "520", "status": "failed", "detail": e.detail}

    unique_id = generate_unique_id()

    try:
        results = await send_messages(
            token=user_data.register_app__token,
            phone_number_id=user_data.phone_number_id,
            template_name=request.template_name,
            language=request.language,
            media_type=request.media_type,
            media_id=request.media_id,
            contact_list=request.contact_list,
            variable_list=request.variable_list,
            unique_id=unique_id
        )

        successful_sends = len([r for r in results if r['status'] == 'success'])
        failed_sends = len([r for r in results if r['status'] == 'failed'])

        if successful_sends > 0:
            report_id = await update_balance_and_report(
                user_id=request.user_id,
                api_token=request.api_token,
                coins=successful_sends,
                contact_list=request.contact_list,
                template_name=request.template_name,
                category=category
            )
        else:
            report_id = None

        return {
            "status": "completed",
            "summary": {
                "total_contacts": total_contacts,
                "successful_sends": successful_sends,
                "failed_sends": failed_sends,
                "remaining_coins": user_data.coins - successful_sends
            },
            "category": category,
            "report_id": report_id,
            "detailed_results": results
        }

    except Exception as e:
        logger.error(f"Message sending failed: {str(e)}")
        return {
            "status": "failed",
            "error_code": "530",
            "detail": "Failed to send messages",
            "error": str(e)
        }
        
@app.get("/send_sms_api/")
async def send_sms_get(
    user_id: str,
    api_token: str,
    template_name: str,
    language: str,
    media_type: str,
    media_id: Optional[str] = None,
    contact_list: str = "",
    variable_list: Optional[str] = None
):
    contact_list_parsed = contact_list.split(",") if contact_list else []
    variable_list_parsed = variable_list.split(",") if variable_list else None

    # Build the same Pydantic model to reuse the POST logic
    request_model = APIMessageRequest(
        user_id=user_id,
        api_token=api_token,
        template_name=template_name,
        language=language,
        media_type=media_type,
        media_id=media_id,
        contact_list=contact_list_parsed,
        variable_list=variable_list_parsed
    )

    # Reuse the same logic
    return await send_sms_post(request_model)

@app.post("/validate_numbers_api/")
async def validate_numbers_api(request: ValidateNumbers, background_tasks: BackgroundTasks):
    try:
        unique_id = generate_unique_id()
        # Schedule the background task and return immediately
        background_tasks.add_task(
            validate_numbers_async,
            token=request.token,
            phone_number_id=request.phone_number_id,
            contact_list=request.contact_list,
            message_text=request.body_text,
            unique_id=unique_id,
            report_id= request.report_id
        )
        return {
            "message": "Task is being processed in the background. You will be notified when it's complete.",
            "unique_id": unique_id,
            "report_id": request.report_id
        }
    except HTTPException as e:
        logger.error(f"HTTP error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

@app.post("/balance_check_api/")
async def balance_check_api(request: APIBalanceRequest):
    try:
        user_data = await fetch_user_data(request.user_id, request.api_token)
        logger.info(f"User validation successful for user_id: {request.user_id}")
        return {
            "Marketing coins": user_data.marketing_coins,
            "Authentication coins": user_data.authentication_coins,
            "Total Balance": user_data.marketing_coins + user_data.authentication_coins
        }
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "540","status": "failed", "detail": e.detail}
    
@app.post("/media_api/")
async def media_api(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    api_token: str = Form(...)
):
    try:
        # Save the uploaded file to the temporary folder
        file_path = os.path.join(TEMP_FOLDER, file.filename)
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        # Fetch user data
        user_data = await fetch_user_data(user_id, api_token)
        token = user_data.register_app__token
        phone_id = user_data.phone_number_id

        # Generate media ID using the saved file
        media_id = await generate_media_id(file_path, token, phone_id)

        # Optionally, delete the file after processing
        os.remove(file_path)

        if media_id:
            return {"media_id": media_id}
        else:
            raise HTTPException(status_code=400, detail="Failed to generate media ID")
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "540", "status": "failed", "detail": e.detail}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/get_user_responses")
async def get_user_responses(
    user_id: str = Query(..., description="User ID from query string"),
    api_token: str = Query(..., description="API Token from query string")
):
    try:
       
        user_data = await fetch_user_data(user_id, api_token)
        app_id = user_data.register_app__app_id
        phone_number_id = user_data.phone_number_id

      
        try:
            connection = mysql.connector.connect(
                host='localhost',
                port='3306',
                user='prashanth@itsolution4india.com',
                password='Solution@97',
                database='webhook_responses',
                auth_plugin='mysql_native_password'
            )
            cursor = connection.cursor(dictionary=True)

            query = f"""
                SELECT `Date`,
                       `display_phone_number`,
                       `phone_number_id`,
                       `waba_id`,
                       `contact_wa_id`,
                       `status`,
                       `message_timestamp`,
                       `error_code`,
                       `error_message`,
                       `contact_name`,
                       `message_from`,
                       `message_type`,
                       `message_body`
                FROM webhook_responses_{app_id}
                WHERE status = 'reply' AND phone_number_id = %s
                ORDER BY `Date` DESC
            """

            cursor.execute(query, (phone_number_id,))
            rows = cursor.fetchall()

            cursor.close()
            connection.close()

        except mysql.connector.Error as err:
            logger.error(f"MySQL Error: {str(err)}")
            raise HTTPException(status_code=500, detail="600, Database connection error")

        # Step 3: Return structured JSON
        return {
            "status": "success",
            "app_id": app_id,
            "phone_number_id":phone_number_id,
            "webhook_responses": rows
        }

    except Exception as e:
        logger.exception("Error in get_user_responses")
        raise HTTPException(status_code=500, detail=f"Error fetching user data: {e}")

if __name__ == '__main__':
    logger.info("Starting the FastAPI server")
    uvicorn.run(app, host="127.0.0.1", port=8000)
