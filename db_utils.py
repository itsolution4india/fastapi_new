import json
from typing import List, Optional
import re
from utils import logger
from db_models import SessionLocal, ReportInfo
from datetime import datetime
import asyncio

def extract_wamid(response_text: str) -> Optional[str]:
    try:
        response_data = json.loads(response_text)
        if "messages" in response_data and len(response_data["messages"]) > 0:
            return response_data["messages"][0]["id"]
    except (json.JSONDecodeError, KeyError, IndexError):
        pass
    
    wamid_pattern = r'wamid\.[A-Za-z0-9+/=]+'
    match = re.search(wamid_pattern, response_text)
    return match.group(0) if match else None


async def save_wamids_to_db(
    wamids: List[str], 
    request_id: str, 
    template_name: str,
) -> None:
    wamid_str = ",".join(wamids)
    
    # Run database operations in a separate thread to avoid blocking
    def db_operation():
        db = SessionLocal()
        try:
            # Check if record with this request_id exists
            report_info = db.query(ReportInfo).filter(
                ReportInfo.start_request_id == int(request_id)
            ).first()
            
            if report_info:
                # Update existing record
                report_info.waba_id_list = wamid_str
                report_info.message_delivery = len(wamids)
            else:
                # Create new record
                new_report = ReportInfo(
                    email="nan",
                    campaign_title='nan',
                    contact_list="",
                    waba_id_list=wamid_str,
                    message_date=datetime.now().date(),
                    template_name=template_name,
                    message_delivery=len(wamids),
                    start_request_id=int(request_id),
                    end_request_id=int(request_id)
                )
                db.add(new_report)
            
            db.commit()
            logger.info(f"Successfully saved {len(wamids)} WAMIDs for request {request_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving WAMIDs to database: {e}", exc_info=True)
        finally:
            db.close()
    
    # Run in threadpool to avoid blocking the event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db_operation)