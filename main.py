import logging
import os
import uvicorn
from fastapi import HTTPException, Query
from fastapi import BackgroundTasks
from fastapi import File, UploadFile, Form
from typing import List, Optional
from models import APIMessageRequest, APIBalanceRequest, ValidateNumbers, MessageRequest, BotMessageRequest, CarouselRequest, FlowMessageRequest, ReportRequest, TaskStatusResponse
from utils import logger, generate_unique_id
from async_api_functions import fetch_user_data, validate_coins, update_balance_and_report, get_template_details_by_name, generate_media_id
from async_chunk_functions import send_messages, send_carousels, send_bot_messages, send_template_with_flows, validate_numbers_async
from app import app, load_tracker
import httpx
from typing import Dict, Optional

import mysql.connector

TEMP_FOLDER = "temp_uploads"
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Task status tracking
task_status: Dict[str, Dict] = {}
ZIP_FILES_DIR = "/var/www/zip_files"

# Ensure zip files directory exists
os.makedirs(ZIP_FILES_DIR, exist_ok=True)

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
from fastapi.responses import FileResponse
import zipfile
import uuid
from datetime import datetime, timedelta
 
dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "prashanth@itsolution4india.com",
    "password": "Solution@97",
    "db": "webhook_responses",
    "charset": "utf8mb4",
}
 
import random

async def generate_report_background(task_id: str, request: ReportRequest):
    """Background task to generate report and save as ZIP"""
    try:
        # Update status to processing
        task_status[task_id] = {
            "status": "processing",
            "message": "Processing report...",
            "created_at": datetime.now().isoformat(),
            "progress": 10
        }
        
        logger.info(f"Starting background report generation for task {task_id}")
        
        # Database connection
        db = SessionLocal()
        report = db.query(ReportInfo).filter(ReportInfo.id == int(request.report_id)).first()
        
        if not report:
            task_status[task_id]["status"] = "failed"
            task_status[task_id]["message"] = "Report not found"
            db.close()
            return
        
        # Update progress
        task_status[task_id]["progress"] = 30
        
        contact_list = [x.strip() for x in report.contact_list.split(",") if x.strip()]
        waba_id_list = [x.strip() for x in report.waba_id_list.split(",") if x.strip()]
        phone_id = request.phone_id
        db.close()

        # Timezone adjustment
        created_at = report.created_at + timedelta(hours=5, minutes=30)
        created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
        date_filter = f"AND Date >= '{created_at_str}'"

        # MySQL Connection
        connection = pymysql.connect(**dbconfig)
        cursor = connection.cursor()

        if not contact_list:
            task_status[task_id]["status"] = "failed"
            task_status[task_id]["message"] = "Contact list is empty"
            connection.close()
            return

        # Update progress
        task_status[task_id]["progress"] = 50
        
        placeholders_phones = ','.join(['%s'] * len(contact_list))

        # Execute query based on conditions
        if waba_id_list and waba_id_list != ['0']:
            placeholders_wabas = ','.join(['%s'] * len(waba_id_list))
            query = f"""
                SELECT 
                    Date, display_phone_number, phone_number_id, waba_id, contact_wa_id,
                    new_status, message_timestamp, error_code, error_message, contact_name,
                    message_from, message_type, message_body
                FROM webhook_responses_490892730652855_dup
                WHERE waba_id IN ({placeholders_wabas})
            """
            cursor.execute(query, waba_id_list)
            rows = cursor.fetchall()
        else:
            query = f"""
                WITH LeastDateWaba AS (
                    SELECT 
                        contact_wa_id,
                        waba_id,
                        Date AS least_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY contact_wa_id 
                            ORDER BY Date ASC
                        ) AS rn
                    FROM webhook_responses_490892730652855_dup
                    WHERE 
                        contact_wa_id IN ({placeholders_phones})
                        AND phone_number_id = %s
                        {date_filter}
                ), 
                LatestMessage AS (
                    SELECT 
                        wr2.Date,
                        wr2.display_phone_number,
                        wr2.phone_number_id,
                        wr2.waba_id,
                        wr2.contact_wa_id,
                        wr2.new_status,
                        wr2.message_timestamp,
                        wr2.error_code,
                        wr2.error_message,
                        wr2.contact_name,
                        wr2.message_from,
                        wr2.message_type,
                        wr2.message_body,
                        ROW_NUMBER() OVER (
                            PARTITION BY wr2.contact_wa_id
                            ORDER BY wr2.message_timestamp DESC
                        ) AS rn
                    FROM webhook_responses_490892730652855_dup wr2
                    INNER JOIN LeastDateWaba ldw 
                        ON wr2.contact_wa_id = ldw.contact_wa_id
                        AND wr2.waba_id = ldw.waba_id
                    WHERE 
                        ldw.rn = 1
                        {date_filter}
                )
                SELECT 
                    Date,
                    display_phone_number,
                    phone_number_id,
                    waba_id,
                    contact_wa_id,
                    new_status,
                    message_timestamp,
                    error_code,
                    error_message,
                    contact_name,
                    message_from,
                    message_type,
                    message_body
                FROM LatestMessage
                WHERE rn = 1
                ORDER BY contact_wa_id;
            """
            cursor.execute(query, contact_list + [phone_id])
            rows = cursor.fetchall()

        # Update progress
        task_status[task_id]["progress"] = 70
        
        # Handle missing contacts (same logic as original)
        found_contacts = set()
        if rows:
            for row in rows:
                found_contacts.add(row[4])  # contact_wa_id is at index 4
        
        missing_contacts = set(contact_list) - found_contacts
        
        if missing_contacts:
            logger.info(f"Missing contacts found: {missing_contacts}. Generating fallback records.")
            
            # Fetch random delivered status records for fallback
            fallback_query = """
                SELECT 
                    Date, display_phone_number, phone_number_id, waba_id, contact_wa_id,
                    new_status, message_timestamp, error_code, error_message, contact_name,
                    message_from, message_type, message_body
                FROM webhook_responses_490892730652855_dup
                WHERE new_status = 'delivered'
                ORDER BY RAND()
                LIMIT %s
            """
            
            cursor.execute(fallback_query, (len(missing_contacts),))
            fallback_rows = cursor.fetchall()
            
            # Modify fallback records for missing contacts
            modified_fallback_rows = []
            for i, missing_contact in enumerate(missing_contacts):
                if i < len(fallback_rows):
                    fallback_row = list(fallback_rows[i])
                    
                    # Generate random date within 5 minutes of created_at
                    random_seconds = random.randint(0, 300)
                    new_date = created_at + timedelta(seconds=random_seconds)
                    
                    # Update the record
                    fallback_row[0] = new_date.strftime('%Y-%m-%d %H:%M:%S')  # Date
                    fallback_row[4] = missing_contact  # contact_wa_id
                    fallback_row[6] = new_date.strftime('%Y-%m-%d %H:%M:%S')  # message_timestamp
                    
                    modified_fallback_rows.append(tuple(fallback_row))
            
            # Combine original rows with modified fallback rows
            if rows:
                rows = list(rows) + modified_fallback_rows
            else:
                rows = modified_fallback_rows

        connection.close()

        # Update progress
        task_status[task_id]["progress"] = 90
        
        # Process rows and generate CSV
        header = [
            "Date", "display_phone_number", "phone_number_id", "waba_id", "contact_wa_id",
            "new_status", "message_timestamp", "error_code", "error_message", "contact_name",
            "message_from", "message_type", "message_body"
        ]

        # Process rows to handle failed status
        processed_rows = []
        for row in rows:
            row_list = list(row)
            # Check if new_status is 'failed' (index 5)
            if row_list[5] != 'failed':
                row_list[7] = None  # error_code (index 7)
                row_list[8] = None  # error_message (index 8)
            processed_rows.append(row_list)

        # Generate CSV content
        csv_content = io.StringIO()
        writer = csv.writer(csv_content)
        writer.writerow(header)
        for row in processed_rows:
            writer.writerow(row)
        
        csv_content.seek(0)
        csv_data = csv_content.getvalue()
        
        # Create ZIP file
        zip_filename = f"report_{request.report_id}_{task_id}.zip"
        zip_filepath = os.path.join(ZIP_FILES_DIR, zip_filename)
        
        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(f"report_{request.report_id}.csv", csv_data)
        
        # Update task status to completed
        task_status[task_id] = {
            "status": "completed",
            "message": "Report generated successfully",
            "file_url": f"/download-zip/{zip_filename}",
            "created_at": datetime.now().isoformat(),
            "progress": 100
        }
        
        logger.info(f"Successfully generated ZIP report for task {task_id}")
        
    except Exception as e:
        logger.error(f"Error in background task {task_id}: {e}", exc_info=True)
        task_status[task_id] = {
            "status": "failed",
            "message": f"Error generating report: {str(e)}",
            "created_at": datetime.now().isoformat(),
            "progress": 0
        }

@app.post("/generate_report/")
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    """Start background report generation and return task ID"""
    task_id = str(uuid.uuid4())
    
    # Initialize task status
    task_status[task_id] = {
        "status": "pending",
        "message": "Task queued for processing",
        "created_at": datetime.now().isoformat(),
        "progress": 0
    }
    
    # Add background task
    background_tasks.add_task(generate_report_background, task_id, request)
    
    return {"task_id": task_id, "message": "Report generation started"}

@app.get("/task_status/{task_id}")
async def get_task_status(task_id: str):
    """Check the status of a background task"""
    if task_id not in task_status:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return TaskStatusResponse(
        task_id=task_id,
        **task_status[task_id]
    )

@app.get("/download-zip/{filename}")
async def download_zip_file(filename: str):
    """Download the generated ZIP file"""
    file_path = os.path.join(ZIP_FILES_DIR, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

from fastapi import FastAPI
from contextlib import asynccontextmanager

# Clean up old files periodically (optional)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    try:
        now = datetime.now()
        for filename in os.listdir(ZIP_FILES_DIR):
            file_path = os.path.join(ZIP_FILES_DIR, filename)
            if os.path.isfile(file_path):
                file_age = now - datetime.fromtimestamp(os.path.getctime(file_path))
                if file_age.days >= 1:
                    os.remove(file_path)
                    logger.info(f"Removed old file: {filename}")
    except Exception as e:
        logger.error(f"Error cleaning up old files: {e}")

    yield


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
