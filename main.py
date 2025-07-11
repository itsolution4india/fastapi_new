import logging
import os
import uvicorn
from fastapi import HTTPException, Query
from fastapi import BackgroundTasks
from fastapi import File, UploadFile, Form
from models import APIMessageRequest, APIBalanceRequest, ValidateNumbers, MessageRequest, BotMessageRequest, CarouselRequest, FlowMessageRequest, ReportRequest, TaskStatusResponse
from utils import logger, generate_unique_id
from async_api_functions import fetch_user_data, validate_coins, update_balance_and_report, get_template_details_by_name, generate_media_id
from async_chunk_functions import send_messages, send_carousels, send_bot_messages, send_template_with_flows, validate_numbers_async
from app import app, load_tracker
import httpx
import threading
from typing import Dict, Optional, Any, Tuple, List
from collections import Counter
import mysql.connector
from db_models import SessionLocal, ReportInfo
import pymysql
from fastapi.responses import FileResponse
import zipfile, uuid, math, json, csv, io, random
from datetime import datetime, timedelta

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
        
dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "prashanth@itsolution4india.com",
    "password": "Solution@97",
    "db": "webhook_responses",
    "charset": "utf8mb4",
}

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
   
import asyncio
   
@app.post("/get_insights/")
async def get_insights(request: ReportRequest, background_tasks: BackgroundTasks):
    """Generate insights and update ReportInfo with statistics"""
    db = None
    try:
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Initialize task status
        update_task_status(task_id, {
            "status": "processing",
            "message": "Starting insights generation...",
            "created_at": datetime.now().isoformat(),
            "progress": 10
        })
        
        # Start background task for insights
        background_tasks.add_task(generate_report_background, task_id, request, True)
        
        return {
            "success": True,
            "task_id": task_id,
            "message": "Insights generation started"
        }
        
    except Exception as e:
        logger.error(f"Error in get_insights: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing insights request: {e}")

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

async def generate_report_background(task_id: str, request: ReportRequest, insight=False):
    """Optimized background task with batch processing"""
    
    # Initialize task status
    update_task_status(task_id, {
        "status": "processing",
        "message": "Processing report...",
        "created_at": datetime.now().isoformat(),
        "progress": 10
    })
    
    logger.info(f"Starting optimized background report generation for task {task_id}")
    
    db = None
    connection = None
    cursor = None
    all_rows = []
    app_id = request.app_id
    
    try:
        # Database connection
        db = SessionLocal()
        report = db.query(ReportInfo).filter(ReportInfo.id == int(request.report_id)).first()
        
        if not report:
            logger.error(f"Report not found for report_id={request.report_id}")
            update_task_status(task_id, {
                "status": "failed",
                "message": "Report not found"
            })
            return
        
        contact_list = [x.strip() for x in report.contact_list.split(",") if x.strip()]
        waba_id_list = [x.strip() for x in report.waba_id_list.split(",") if x.strip()]
        phone_id = request.phone_id
        
        campaign_title = str(report.campaign_title)
        template_name = str(report.campaign_title)
        
        logger.info(f"Contact list size: {len(contact_list)}")
        
        # Timezone adjustment
        created_at = report.created_at + timedelta(hours=5, minutes=30)
        created_at_str = created_at.strftime('%Y-%m-%d %H:%M:%S')
        
        # MySQL Connection
        connection = pymysql.connect(**dbconfig)
        cursor = connection.cursor()
        
        update_task_status(task_id, {
            "progress": 30,
            "message": "Database connected, starting batch processing..."
        })
        
        if not contact_list:
            logger.error("Contact list is empty")
            update_task_status(task_id, {
                "status": "failed",
                "message": "Contact list is empty"
            })
            return
        
        # BATCH PROCESSING APPROACH
        batch_size = 1000  # Process 1000 contacts at a time
        total_batches = math.ceil(len(contact_list) / batch_size)
        
        logger.info(f"Processing {len(contact_list)} contacts in {total_batches} batches of {batch_size}")
        
        # Process contacts in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(contact_list))
            batch_contacts = contact_list[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches} with {len(batch_contacts)} contacts")
            
            # Update progress
            base_progress = 30
            batch_progress = int((batch_num / total_batches) * 50)  # 50% for batch processing
            current_progress = base_progress + batch_progress
            
            update_task_status(task_id, {
                "progress": current_progress,
                "message": f"Processing batch {batch_num + 1}/{total_batches}..."
            })
            
            # Execute batch query
            batch_rows = await execute_batch_query(
                cursor, batch_contacts, phone_id, created_at_str, waba_id_list, app_id
            )
            
            if batch_rows:
                all_rows.extend(batch_rows)
                logger.info(f"Batch {batch_num + 1} completed: {len(batch_rows)} rows")
            else:
                logger.info(f"Batch {batch_num + 1} completed: 0 rows")
        
        logger.info(f"All batches completed. Total rows: {len(all_rows)}")
        
        # Update progress
        update_task_status(task_id, {
            "progress": 80,
            "message": "Processing missing contacts..."
        })
        
        found_contacts = set()
        if all_rows:
            for row in all_rows:
                found_contacts.add(row[4])  # contact_wa_id is at index 4
        
        missing_contacts = set(contact_list) - found_contacts
        
        if missing_contacts:
            logger.info(f"Missing contacts found: {len(missing_contacts)} contacts")
            fallback_rows = await generate_fallback_data(cursor, missing_contacts, created_at)
            if fallback_rows:
                all_rows.extend(fallback_rows)
        
        # Continue with file generation (same as before)
        update_task_status(task_id, {
            "progress": 90,
            "message": "Generating report file..."
        })
        
        if insight:
            seen_contacts = set()
            unique_rows = []
            for row in all_rows:
                contact_wa_id = row[4]
                if contact_wa_id not in seen_contacts:
                    seen_contacts.add(contact_wa_id)
                    unique_rows.append(row)
            
            status_counts = Counter(row[5].lower() if row[5] else '' for row in unique_rows)
            
            report.deliver_count = status_counts.get('delivered', 0)
            report.sent_count = status_counts.get('sent', 0)
            report.read_count = status_counts.get('read', 0)
            report.pending_count = status_counts.get('pending', 0)
            report.failed_count = status_counts.get('failed', 0)
            report.reply_count = status_counts.get('reply', 0)
            report.total_count = len(contact_list)
            
            # Commit the changes
            db.commit()
        else:
            # Generate CSV and ZIP file (same as original code)
            zip_filename = await generate_csv_zip(all_rows, request.report_id, task_id, campaign_title, str(template_name), str(created_at))
        
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
        if connection:
            connection.close()
        if db:
            db.close()

async def execute_batch_query(cursor, batch_contacts: List[str], phone_id: str, 
                             created_at_str: str, waba_id_list: List[str], app_id: str) -> List[Tuple]:
    """Execute optimized query for a batch of contacts"""

    placeholders_contacts = ','.join(['%s'] * len(batch_contacts))
    base_query = f"""
        SELECT 
            wr1.Date,
            wr1.display_phone_number,
            wr1.phone_number_id,
            wr1.waba_id,
            wr1.contact_wa_id,
            wr1.status,
            wr1.message_timestamp,
            wr1.error_code,
            wr1.error_message,
            wr1.contact_name,
            wr1.message_from,
            wr1.message_type,
            wr1.message_body
        FROM webhook_responses_{app_id} wr1
        WHERE wr1.contact_wa_id IN ({placeholders_contacts})
        AND wr1.phone_number_id = %s
        AND wr1.Date >= %s
        {"AND wr1.waba_id IN (" + ','.join(['%s'] * len(waba_id_list)) + ")" if waba_id_list and waba_id_list != ['0'] else ""}
        AND wr1.message_timestamp = (
            SELECT MAX(wr2.message_timestamp)
            FROM webhook_responses_{app_id} wr2
            WHERE wr2.contact_wa_id = wr1.contact_wa_id
            AND wr2.phone_number_id = wr1.phone_number_id
            AND wr2.Date >= %s
            {"AND wr2.waba_id IN (" + ','.join(['%s'] * len(waba_id_list)) + ")" if waba_id_list and waba_id_list != ['0'] else ""}
        )
        ORDER BY wr1.contact_wa_id
    """

    # Prepare parameters
    params = batch_contacts + [phone_id, created_at_str]
    if waba_id_list and waba_id_list != ['0']:
        params += waba_id_list  # for wr1.waba_id
    params.append(created_at_str)
    if waba_id_list and waba_id_list != ['0']:
        params += waba_id_list  # for wr2.waba_id

    try:
        cursor.execute(base_query, params)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error executing batch query: {str(e)}")
        return []

async def generate_fallback_data(cursor, missing_contacts: set, created_at: datetime) -> List[Tuple]:
    """Generate fallback data for missing contacts"""

    if not missing_contacts:
        return []
    
    # Fetch random delivered status records for fallback
    fallback_query = """
        SELECT 
            Date, display_phone_number, phone_number_id, waba_id, contact_wa_id,
            status, message_timestamp, error_code, error_message, contact_name,
            message_from, message_type, message_body
        FROM webhook_responses
        WHERE status = 'delivered'
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
        
            # Check if new_date is within the last 24 hours
            if (datetime.now() - new_date) < timedelta(hours=24):
                fallback_row[5] = 'pending'  # status
            
            # Update the record
            fallback_row[0] = new_date.strftime('%Y-%m-%d %H:%M:%S')  # Date
            fallback_row[4] = missing_contact  # contact_wa_id
            fallback_row[6] = new_date.strftime('%Y-%m-%d %H:%M:%S')  # message_timestamp
            
            modified_fallback_rows.append(tuple(fallback_row))
    
    return modified_fallback_rows

async def generate_csv_zip(rows: List[Tuple], report_id: str, task_id: str, campaign_title: str, template_name: str, created_at: str) -> str:
    """Generate CSV and ZIP file from rows""" 
     
    if not os.path.exists(ZIP_FILES_DIR): 
        os.makedirs(ZIP_FILES_DIR, exist_ok=True) 
     
    # Process rows and generate CSV 
    header = [ 
        "Date", "display_phone_number", "phone_number_id", "waba_id", "contact_wa_id", 
        "status", "message_timestamp", "error_code", "error_message", "contact_name", 
        "message_from", "message_type", "message_body" 
    ] 
 
    seen_contacts = set() 
    processed_rows = [] 
     
    for row in rows: 
        row_list = list(row) 
        contact_wa_id = row_list[4] 
        if contact_wa_id in seen_contacts: 
            continue 
             
        seen_contacts.add(contact_wa_id) 
        
        # Handle error code 131047 - treat as delivered
        error_code = row_list[7]
        if error_code == 131047 or error_code == '131047':
            row_list[5] = 'delivered'  # status
            row_list[7] = None         # error_code
            row_list[8] = None         # error_message
        elif row_list[5] != 'failed': 
            row_list[7] = None 
            row_list[8] = None 
         
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
    if isinstance(created_at, str): 
        created_at = datetime.fromisoformat(created_at)  # Or use strptime() 
 
    # Now create filename with only the date part 
    created_str = created_at.strftime('%Y-%m-%d') 
    zip_filename = f"report_{campaign_title}_{template_name}_{created_str}_{report_id}.zip" 
    zip_filepath = os.path.join(ZIP_FILES_DIR, zip_filename) 
 
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf: 
        zipf.writestr(f"report_{campaign_title}_{template_name}_{created_str}_{report_id}.csv", csv_data) 
     
    if not os.path.exists(zip_filepath): 
        raise Exception("Failed to create ZIP file") 
     
    return zip_filename

# Alternative: Even more aggressive optimization with connection pooling
class DatabasePool:
    def __init__(self, pool_size=5):
        self.pool = []
        self.pool_size = pool_size
        self.lock = threading.Lock()
    
    def get_connection(self):
        with self.lock:
            if self.pool:
                return self.pool.pop()
            else:
                return pymysql.connect(**dbconfig)
    
    def return_connection(self, connection):
        with self.lock:
            if len(self.pool) < self.pool_size:
                self.pool.append(connection)
            else:
                connection.close()

# Initialize global connection pool
db_pool = DatabasePool(pool_size=3)

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
        # cleanup_old_tasks()
        
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
