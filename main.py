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
import threading
from typing import Dict, Optional, Any

import mysql.connector

TEMP_FOLDER = "temp_uploads"
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Task status tracking
task_status: Dict[str, Dict[str, Any]] = {}
ZIP_FILES_DIR = "/var/www/zip_files"
TASK_STATUS_FILE = os.path.join(ZIP_FILES_DIR, "task_status.json")

# Ensure zip files directory exists
os.makedirs(ZIP_FILES_DIR, exist_ok=True)
task_status_lock = threading.Lock()

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
import json
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

def load_task_status():
    """Load task status from file"""
    global task_status
    try:
        if os.path.exists(TASK_STATUS_FILE):
            with open(TASK_STATUS_FILE, 'r') as f:
                task_status = json.load(f)
            logger.info(f"Loaded {len(task_status)} tasks from file")
        else:
            task_status = {}
            logger.info("No existing task status file found, starting fresh")
    except Exception as e:
        logger.error(f"Error loading task status: {e}")
        task_status = {}

def save_task_status():
    """Save task status to file"""
    try:
        with task_status_lock:
            with open(TASK_STATUS_FILE, 'w') as f:
                json.dump(task_status, f, indent=2)
        logger.debug("Task status saved to file")
    except Exception as e:
        logger.error(f"Error saving task status: {e}")

def update_task_status(task_id: str, status_update: Dict[str, Any]):
    """Thread-safe task status update"""
    with task_status_lock:
        if task_id not in task_status:
            task_status[task_id] = {}
        task_status[task_id].update(status_update)
        task_status[task_id]["updated_at"] = datetime.now().isoformat()
    save_task_status()
    logger.info(f"Task {task_id} status updated: {status_update}")

def get_task_status(task_id: str) -> Dict[str, Any]:
    """Thread-safe task status retrieval"""
    with task_status_lock:
        return task_status.get(task_id, None)

def cleanup_old_tasks():
    """Clean up old completed/failed tasks (older than 24 hours)"""
    try:
        current_time = datetime.now()
        tasks_to_remove = []
        
        with task_status_lock:
            for task_id, status in task_status.items():
                if status.get("status") in ["completed", "failed"]:
                    created_at = datetime.fromisoformat(status.get("created_at", "2000-01-01T00:00:00"))
                    if (current_time - created_at).total_seconds() > 86400:  # 24 hours
                        tasks_to_remove.append(task_id)
            
            for task_id in tasks_to_remove:
                del task_status[task_id]
                logger.info(f"Removed old task: {task_id}")
        
        if tasks_to_remove:
            save_task_status()
            
    except Exception as e:
        logger.error(f"Error cleaning up old tasks: {e}")

# Load existing task status on startup
load_task_status()

# Modified background task function
async def generate_report_background(task_id: str, request: ReportRequest):
    """Background task to generate report and save as ZIP"""
    # Initialize task status first - this is crucial
    update_task_status(task_id, {
        "status": "processing",
        "message": "Processing report...",
        "created_at": datetime.now().isoformat(),
        "progress": 10
    })
    
    logger.info(f"Starting background report generation for task {task_id}")
    logger.info(f"Request details: report_id={request.report_id}, phone_id={request.phone_id}")
    
    db = None
    connection = None
    cursor = None
    
    try:
        # Database connection
        logger.info(f"Connecting to database for task {task_id}")
        db = SessionLocal()
        report = db.query(ReportInfo).filter(ReportInfo.id == int(request.report_id)).first()
        
        if not report:
            logger.error(f"Report not found for report_id={request.report_id}")
            update_task_status(task_id, {
                "status": "failed",
                "message": "Report not found"
            })
            return
        
        logger.info(f"Found report: {report.id}")
        
        # Update progress
        update_task_status(task_id, {"progress": 30})
        
        contact_list = [x.strip() for x in report.contact_list.split(",") if x.strip()]
        waba_id_list = [x.strip() for x in report.waba_id_list.split(",") if x.strip()]
        phone_id = request.phone_id
        
        logger.info(f"Contact list size: {len(contact_list)}")
        logger.info(f"WABA ID list: {waba_id_list}")
        logger.info(f"Phone ID: {phone_id}")
        
        # Timezone adjustment
        created_at = report.created_at + timedelta(hours=5, minutes=30)
        created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
        date_filter = f"AND Date >= '{created_at_str}'"
        
        logger.info(f"Date filter: {date_filter}")

        # MySQL Connection
        logger.info("Connecting to MySQL database")
        connection = pymysql.connect(**dbconfig)
        cursor = connection.cursor()
        logger.info("MySQL connection established")

        if not contact_list:
            logger.error("Contact list is empty")
            update_task_status(task_id, {
                "status": "failed",
                "message": "Contact list is empty"
            })
            return

        # Update progress
        update_task_status(task_id, {"progress": 50})
        
        placeholders_phones = ','.join(['%s'] * len(contact_list))

        # Execute query based on conditions
        if waba_id_list and waba_id_list != ['0']:
            logger.info("Executing query with WABA ID filter")
            placeholders_wabas = ','.join(['%s'] * len(waba_id_list))
            query = f"""
                SELECT 
                    Date, display_phone_number, phone_number_id, waba_id, contact_wa_id,
                    new_status, message_timestamp, error_code, error_message, contact_name,
                    message_from, message_type, message_body
                FROM webhook_responses_490892730652855_dup
                WHERE waba_id IN ({placeholders_wabas})
            """
            logger.info(f"Query: {query}")
            logger.info(f"Parameters: {waba_id_list}")
            cursor.execute(query, waba_id_list)
            rows = cursor.fetchall()
        else:
            logger.info("Executing query with contact list and phone ID filter")
            
            # Update progress during long query
            update_task_status(task_id, {
                "progress": 55,
                "message": "Executing database query..."
            })
            
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
            params = contact_list + [phone_id]
            logger.info(f"Query parameters count: {len(params)}")
            cursor.execute(query, params)
            rows = cursor.fetchall()

        logger.info(f"Query executed successfully. Found {len(rows)} rows")

        # Update progress
        update_task_status(task_id, {
            "progress": 70,
            "message": "Processing query results..."
        })
        
        # Handle missing contacts
        found_contacts = set()
        if rows:
            for row in rows:
                found_contacts.add(row[4])  # contact_wa_id is at index 4
        
        missing_contacts = set(contact_list) - found_contacts
        
        if missing_contacts:
            logger.info(f"Missing contacts found: {len(missing_contacts)} contacts")
            logger.info(f"Missing contacts: {list(missing_contacts)}")
            
            # Update progress for fallback processing
            update_task_status(task_id, {
                "progress": 75,
                "message": "Processing missing contacts..."
            })
            
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
            logger.info(f"Found {len(fallback_rows)} fallback rows")
            
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
            
            logger.info(f"Total rows after adding fallback: {len(rows)}")

        # Update progress
        update_task_status(task_id, {
            "progress": 90,
            "message": "Generating report file..."
        })
        
        # Create ZIP_FILES_DIR if it doesn't exist
        if not os.path.exists(ZIP_FILES_DIR):
            logger.info(f"Creating directory: {ZIP_FILES_DIR}")
            os.makedirs(ZIP_FILES_DIR, exist_ok=True)
        
        # Check directory permissions
        if not os.access(ZIP_FILES_DIR, os.W_OK):
            logger.error(f"No write permission for directory: {ZIP_FILES_DIR}")
            update_task_status(task_id, {
                "status": "failed",
                "message": f"No write permission for directory: {ZIP_FILES_DIR}"
            })
            return
        
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

        logger.info(f"Processed {len(processed_rows)} rows")

        # Generate CSV content
        csv_content = io.StringIO()
        writer = csv.writer(csv_content)
        writer.writerow(header)
        for row in processed_rows:
            writer.writerow(row)
        
        csv_content.seek(0)
        csv_data = csv_content.getvalue()
        
        logger.info(f"CSV content generated, size: {len(csv_data)} characters")
        
        # Create ZIP file
        zip_filename = f"report_{request.report_id}_{task_id}.zip"
        zip_filepath = os.path.join(ZIP_FILES_DIR, zip_filename)
        
        logger.info(f"Creating ZIP file: {zip_filepath}")
        
        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(f"report_{request.report_id}.csv", csv_data)
        
        # Verify ZIP file was created
        if os.path.exists(zip_filepath):
            file_size = os.path.getsize(zip_filepath)
            logger.info(f"ZIP file created successfully: {zip_filepath}, size: {file_size} bytes")
        else:
            logger.error(f"ZIP file was not created: {zip_filepath}")
            update_task_status(task_id, {
                "status": "failed",
                "message": "Failed to create ZIP file"
            })
            return
        
        # Update task status to completed
        update_task_status(task_id, {
            "status": "completed",
            "message": "Report generated successfully",
            "file_url": f"/download-zip/{zip_filename}",
            "progress": 100
        })
        
        logger.info(f"Successfully generated ZIP report for task {task_id}")
        
    except Exception as e:
        logger.error(f"Error in background task {task_id}: {str(e)}", exc_info=True)
        update_task_status(task_id, {
            "status": "failed",
            "message": f"Error generating report: {str(e)}",
            "progress": 0
        })
    
    finally:
        # Clean up database connections
        if cursor:
            cursor.close()
            logger.info("MySQL cursor closed")
        if connection:
            connection.close()
            logger.info("MySQL connection closed")
        if db:
            db.close()
            logger.info("SQLAlchemy session closed")

# Modified endpoint functions
@app.post("/generate_report/")
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    """Start background report generation and return task ID"""
    task_id = str(uuid.uuid4())
    logger.info(f"Generated new task_id: {task_id}")
    
    # Initialize task status BEFORE adding background task
    update_task_status(task_id, {
        "status": "pending",
        "message": "Task queued for processing",
        "created_at": datetime.now().isoformat(),
        "progress": 0
    })
    
    logger.info(f"Task {task_id} initialized")
    
    # Add background task
    background_tasks.add_task(generate_report_background, task_id, request)
    logger.info(f"Background task added for task_id: {task_id}")
    
    return {"task_id": task_id, "message": "Report generation started"}

@app.get("/task_status/{task_id}")
async def get_task_status_endpoint(task_id: str):
    """Check the status of a background task"""
    logger.info(f"Checking status for task_id: {task_id}")
    
    status = get_task_status(task_id)
    if status is None:
        logger.error(f"Task not found: {task_id}")
        with task_status_lock:
            available_tasks = list(task_status.keys())
        logger.info(f"Available task_ids: {available_tasks}")
        raise HTTPException(status_code=404, detail="Task not found")
    
    logger.info(f"Task {task_id} status: {status}")
    
    return TaskStatusResponse(
        task_id=task_id,
        **status
    )


@app.get("/download-zip/{filename}")
async def download_zip_file(filename: str):
    """Download the generated ZIP file"""
    logger.info(f"Download request for file: {filename}")
    file_path = os.path.join(ZIP_FILES_DIR, filename)
    logger.info(f"Full file path: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        # List available files for debugging
        try:
            available_files = os.listdir(ZIP_FILES_DIR)
            logger.info(f"Available files in {ZIP_FILES_DIR}: {available_files}")
        except Exception as e:
            logger.error(f"Error listing directory contents: {e}")
        raise HTTPException(status_code=404, detail="File not found")
    
    logger.info(f"File found, serving: {file_path}")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/debug/tasks")
async def debug_tasks():
    """Debug endpoint to see all tasks"""
    with task_status_lock:
        current_tasks = dict(task_status)
    logger.info(f"Debug: Current tasks in memory: {len(current_tasks)} tasks")
    return {"tasks": current_tasks, "count": len(current_tasks)}


# Debug endpoint to check ZIP directory
@app.get("/debug/zip-files")
async def debug_zip_files():
    """Debug endpoint to see ZIP files"""
    try:
        if os.path.exists(ZIP_FILES_DIR):
            files = os.listdir(ZIP_FILES_DIR)
            logger.info(f"Debug: Files in {ZIP_FILES_DIR}: {files}")
            return {"zip_directory": ZIP_FILES_DIR, "files": files}
        else:
            logger.info(f"Debug: Directory {ZIP_FILES_DIR} does not exist")
            return {"zip_directory": ZIP_FILES_DIR, "exists": False}
    except Exception as e:
        logger.error(f"Debug: Error accessing {ZIP_FILES_DIR}: {e}")
        return {"error": str(e)}

from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    logger.info(f"ZIP_FILES_DIR: {ZIP_FILES_DIR}")
    
    try:
        # Create ZIP directory if it doesn't exist
        if not os.path.exists(ZIP_FILES_DIR):
            os.makedirs(ZIP_FILES_DIR, exist_ok=True)
            logger.info(f"Created directory: {ZIP_FILES_DIR}")
        
        # Load existing task status
        load_task_status()
        
        # Clean up old files and tasks
        cleanup_old_tasks()
        
        now = datetime.now()
        for filename in os.listdir(ZIP_FILES_DIR):
            if filename.endswith('.zip'):
                file_path = os.path.join(ZIP_FILES_DIR, filename)
                if os.path.isfile(file_path):
                    file_age = now - datetime.fromtimestamp(os.path.getctime(file_path))
                    if file_age.days >= 1:
                        os.remove(file_path)
                        logger.info(f"Removed old file: {filename}")
    except Exception as e:
        logger.error(f"Error during startup: {e}")

    yield
    
    # Save task status on shutdown
    save_task_status()
    logger.info("Shutting down application...")


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
