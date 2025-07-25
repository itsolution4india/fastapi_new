import logging
import os
import typing as ty
import aiofiles
import aiohttp
import uvicorn
import asyncio
import json
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Request
from typing import Optional, List
import httpx
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks
import random
from fastapi import File, UploadFile, Form
import httpx
from aiohttp import FormData

# Directory and file for logging
log_directory = "logs"
log_file = "app.log"

TEMP_FOLDER = "temp_uploads"
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Ensure log directory exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Create a custom logging handler to ensure proper logging in async applications
class AsyncFileHandler(logging.FileHandler):
    async def emit(self, record):
        msg = self.format(record)
        async with aiofiles.open(self.baseFilename, 'a') as f:
            await f.write(msg + '\n')

# Configure logging to log to both console and a file
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more verbose logging
    format="%(asctime)s [%(levelname)s] %(message)s",  # Include timestamp
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),  # Log to console (stdout)
        logging.FileHandler(os.path.join(log_directory, log_file))  # Log to file
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Define the Pydantic model for request body
class APIMessageRequest(BaseModel):
    user_id: str
    api_token: str
    template_name: str
    language: str
    media_type: str
    media_id: ty.Optional[str]
    contact_list: ty.List[str]
    variable_list: ty.Optional[ty.List[str]] = None
    
class ValidateNumbers(BaseModel):
    token: str
    phone_number_id: str
    contact_list: ty.List[str]
    body_text: str
    report_id: Optional[str] = None

class UserData(BaseModel):
    whatsapp_business_account_id: str
    phone_number_id: str
    register_app__app_id: str
    register_app__token: str
    coins: int
    marketing_coins: int
    authentication_coins: int

class APIBalanceRequest(BaseModel):
    user_id: str
    api_token: str
    
class MediaID(BaseModel):
    user_id: str
    api_token: str

class UpdateBalanceReportRequest(BaseModel):
    user_id: str
    api_token: str
    coins: int
    phone_numbers: str
    all_contact: ty.List[int]
    template_name: str

def generate_unique_id() -> str:
    """Generate a 16-digit unique number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

async def fetch_user_data(user_id: str, api_token: str) -> UserData:
    """Fetch user data from the API"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://main.wtsmessage.xyz/api/users/")
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail="Failed to connect to user validation service"
                )
            
            users = response.json()
            user = next((u for u in users if u["user_id"] == user_id and u["api_token"] == api_token), None)
            
            if not user:
                raise HTTPException(
                    status_code=401,
                    detail="Failed to validate user credentials. Please check your user_id and api_token"
                )
                
            if not user["is_active"]:
                raise HTTPException(
                    status_code=403,
                    detail="User account is not active. Please contact support"
                )
                
            return UserData(
                whatsapp_business_account_id=user["whatsapp_business_account_id"],
                phone_number_id=user["phone_number_id"],
                register_app__app_id=user["register_app__app_id"],
                register_app__token=user["register_app__token"],
                coins=user["coins"],
                marketing_coins=user["marketing_coins"],
                authentication_coins=user["authentication_coins"]
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in user validation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error during user validation"
        )

async def validate_coins(available_coins: int, required_contacts: int):
    """Validate if user has sufficient coins"""
    if required_contacts > available_coins:
        raise HTTPException(
            status_code=402,  # Using 402 Payment Required
            detail={
                "message": "Insufficient coins. Please recharge your account",
                "available_coins": available_coins,
                "required_coins": required_contacts
            }
        )

async def update_balance_and_report(
    user_id: str,
    api_token: str,
    coins: int,
    contact_list: ty.List[str],
    template_name: str,
    category: str
) -> str:
    """Update balance and create report"""
    try:
        # Prepare phone numbers string and contact list
        phone_numbers = ",".join(contact_list)
        all_contact = [int(phone.strip()) for phone in contact_list]
        
        update_data = UpdateBalanceReportRequest(
            user_id=user_id,
            api_token=api_token,
            coins=coins,
            phone_numbers=phone_numbers,
            all_contact=all_contact,
            template_name=template_name,
            category = category
        )
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://main.wtsmessage.xyz/update-balance-report/",
                json=update_data.dict()
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to update balance and report: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail="Failed to update balance and report"
                )
            
            result = response.json()
            return result["report_id"]
            
    except Exception as e:
        logger.error(f"Error updating balance and report: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to update balance and report"
        )


class CarouselRequest(BaseModel):
    token: str
    phone_number_id: str
    template_name: str
    contact_list: ty.List[str]
    media_id_list: ty.List[str]
    template_details: dict
    request_id: Optional[str] = None
    
class MessageRequest(BaseModel):
    token: str
    phone_number_id: str
    template_name: str
    language: str
    media_type: str
    media_id: ty.Optional[str]
    contact_list: ty.List[str]
    variable_list: ty.Optional[ty.List[str]] = None
    csv_variables: ty.Optional[ty.List[ty.List[str]]] = None
    request_id: Optional[str] = None

class FlowMessageRequest(BaseModel):
    token: str
    phone_number_id: str
    template_name: str
    flow_id: str
    language: str
    recipient_phone_number: ty.List[str]
    request_id: Optional[str] = None

class BotMessageRequest(BaseModel):
    token: str
    phone_number_id: str
    contact_list: List[str]
    message_type: str
    header: Optional[str] = None
    body: Optional[str] = None
    footer: Optional[str] = None
    button_data: Optional[List[dict]] = None
    product_data: Optional[dict] = None
    catalog_id: Optional[str] = None
    sections: Optional[List[dict]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    media_id: Optional[str] = None

async def send_template_with_flow(session: aiohttp.ClientSession, token: str, phone_number_id: str, template_name: str, flow_id: str, language: str, recipient_phone_number: str):
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    data = {
        "messaging_product": "whatsapp",
        "to": recipient_phone_number,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {
                "code": language
            },
            "components": [
                {
                    "type": "button",
                    "sub_type": "flow",
                    "index": "0",
                    "parameters": [
                        {
                            "type": "payload",
                            "payload": flow_id
                        }
                    ]
                }
            ]
        }
    }
    
    logger.info(f"Attempting to send flow message. Data: {json.dumps(data, indent=2)}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=data) as response:
                status_code = response.status
                response_dict = await response.json()
                return status_code, response_dict
        except aiohttp.ClientError as e:
            logger.error(f"Error sending flow message: {e}")
            raise HTTPException(status_code=500, detail=f"Error sending flow message: {e}")

async def send_message(session: aiohttp.ClientSession, token: str, phone_number_id: str, template_name: str, language: str, media_type: str, media_id: ty.Optional[str], contact: str, variables: ty.Optional[ty.List[str]] = None, csv_variable_list: ty.Optional[ty.List[str]] = None) -> None:
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    header_component = {
        "type": "header",
        "parameters": []
    }

    body_component = {
        "type": "body",
        "parameters": []
    }
    
    if csv_variable_list:
        variables = csv_variable_list[1:]
        contact = str(csv_variable_list[0])
    
    if variables:
        body_component["parameters"] = [
            {
                "type": "text",
                "text": variable
            } for variable in variables
        ]

    context_info = json.dumps({
        "template_name": template_name,
        "language": language,
        "media_type": media_type
    })

    if media_id and media_type in ["IMAGE", "DOCUMENT", "VIDEO", "AUDIO"]:
        header_component["parameters"].append({
            "type": media_type.lower(),
            media_type.lower(): {"id": media_id}
        })

    payload = {
        "messaging_product": "whatsapp",
        "to": contact,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
            "components": [
                header_component,
                body_component
            ]
        },
        "context": {
            "message_id": f"template_{template_name}_{context_info}"
        }
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            response_text = await response.text()
            if response.status == 200:
                return {
                    "status": "success",
                    "contact": contact,
                    "message_id": f"template_{template_name}_{context_info}",
                    "response": response_text
                }
            else:
                logger.error(f"Failed to send message to {contact}. Status: {response.status}, Error: {response_text}")
                return {
                    "status": "failed",
                    "contact": contact,
                    "error_code": response.status,
                    "error_message": response_text
                }
    except aiohttp.ClientError as e:
        logger.error(f"Error sending message to {contact}: {e}")
        return {
            "status": "failed",
            "contact": contact,
            "error_code": "client_error",
            "error_message": str(e)
        }
        
async def send_carousel(
    session: aiohttp.ClientSession, 
    token: str, 
    phone_number_id: str, 
    template_name: str, 
    contact: str, 
    media_id_list: ty.List[str], 
    template_details: dict
) -> dict:
    url = f"https://graph.facebook.com/v21.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    cards = []
    for idx, media_id in enumerate(media_id_list):
        card = {
            "card_index": idx,
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "image",
                            "image": {
                                "id": media_id
                            }
                        }
                    ]
                },
                {
                    "type": "button",
                    "sub_type": "quick_reply",
                    "index": "0",
                    "parameters": [
                        {
                            "type": "payload",
                            "payload": f"more-item-{idx}"
                        }
                    ]
                },
                {
                    "type": "button",
                    "sub_type": "url",
                    "index": "1",
                    "parameters": [
                        {
                            "type": "text",
                            "text": f"url-item-{idx}"
                        }
                    ]
                }
            ]
        }
        cards.append(card)
    
    carousel_message = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": contact,
        "type": "template",
        "template": {
            "name": template_details['template_name'],
            "language": {
                "code": template_details['template_language']
            },
            "components": [
                {
                    "type": "body"
                },
                {
                    "type": "carousel",
                    "cards": cards
                }
            ]
        }
    }

    try:
        async with session.post(url, headers=headers, json=carousel_message) as response:
            response_text = await response.text()
            if response.status == 200:
                return {
                    "status": "success",
                    "contact": contact,
                    "message_id": f"template_{template_name}",
                    "response": response_text
                }
            else:
                logger.error(f"Failed to send message to {contact}. Status: {response.status}, Error: {response_text}")
                return {
                    "status": "failed",
                    "contact": contact,
                    "error_code": response.status,
                    "error_message": response_text
                }
    except aiohttp.ClientError as e:
        logger.error(f"Error sending message to {contact}: {e}")
        return {
            "status": "failed",
            "contact": contact,
            "error_code": "client_error",
            "error_message": str(e)
        }

async def send_otp_message(session: aiohttp.ClientSession, token: str, phone_number_id: str, template_name: str, language: str, media_type: str, media_id: ty.Optional[str], contact: str, variables: ty.Optional[ty.List[str]] = None, csv_variable_list: ty.Optional[ty.List[str]] = None) -> None:
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    header_component = {
        "type": "header",
        "parameters": []
    }

    body_component = {
        "type": "body",
        "parameters": []
    }

    button_component = {
        "type": "button",
        "sub_type": "url",
        "index": "0",
        "parameters": []
    }

    if csv_variable_list:
        variables = csv_variable_list[1:]
        contact = str(csv_variable_list[0])
    
    if variables:
        body_component["parameters"] = [
            {
                "type": "text",
                "text": variable
            } for variable in variables
        ]

    if media_id and media_type in ["IMAGE", "DOCUMENT", "VIDEO", "AUDIO"]:
        header_component["parameters"].append({
            "type": media_type.lower(),
            media_type.lower(): {"id": media_id}
        })

    button_url = "https://www.whatsapp.com/otp/code/?otp_type=COPY_CODE&code_expiration_minutes=10&code=otp123456"
    button_component["parameters"].append({
        "type": "text",
        "text": variables[0]
    })

    payload = {
        "messaging_product": "whatsapp",
        "to": contact,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
            "components": [
                header_component,
                body_component,
                button_component
            ]
        },
        "context": {
            "message_id": f"template_{template_name}_{json.dumps({'template_name': template_name, 'language': language, 'media_type': media_type})}"
        }
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            response_text = await response.text()
            if response.status == 200:
                return {
                    "status": "success",
                    "contact": contact,
                    "message_id": f"template_{template_name}",
                    "response": response_text
                }
            else:
                logger.error(f"Failed to send message to {contact}. Status: {response.status}, Error: {response_text}")
                return {
                    "status": "failed",
                    "contact": contact,
                    "error_code": response.status,
                    "error_message": response_text
                }
    except aiohttp.ClientError as e:
        logger.error(f"Error sending message to {contact}: {e}")
        return {
            "status": "failed",
            "contact": contact,
            "error_code": "client_error",
            "error_message": str(e)
        }

async def validate_nums(session: aiohttp.ClientSession, token: str, phone_number_id: str, contact: str, message_text: str):
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messaging_product": "whatsapp",
        "to": contact,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message_text
        }
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            response_text = await response.text()
            if response.status == 200:
                return {
                    "status": "success",
                    "response_text": response_text
                }
            else:
                return {
                    "status": "failed",
                    "response_text": response_text
                }
    except Exception as e:
        logger.error(f"Error sending to {contact}: {e}")
        return {"status": "error", "message": str(e)}

async def send_bot_message(session: aiohttp.ClientSession, token: str, phone_number_id: str, contact: str, message_type: str, header: ty.Optional[str] = None, body: ty.Optional[str] = None, footer: ty.Optional[str] = None, button_data: ty.Optional[ty.List[ty.Dict[str, str]]] = None, product_data: ty.Optional[ty.Dict] = None, catalog_id: ty.Optional[str] = None, sections: ty.Optional[ty.List[ty.Dict]] = None, latitude: ty.Optional[float] = None, longitude: ty.Optional[float] = None, media_id: ty.Optional[str] = None ) -> None:
    url = f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messaging_product": "whatsapp",
        "to": contact,
        "type": "interactive"
    }

    if message_type == "text":
        payload["type"] = "text"
        payload["text"] = {
            "preview_url": False,
            "body": body
        }

    elif message_type == "image":
        payload["type"] = "image"
        payload["image"] = {
            "id": media_id,
            "caption": body if body else None
        }

    elif message_type == "document":
        payload["type"] = "document"
        payload["document"] = {
            "id": media_id,
            "caption": body if body else None,
            "filename": header if header else "document"
        }

    elif message_type == "video":
        payload["type"] = "video"
        payload["video"] = {
            "id": media_id,
            "caption": body if body else None
        }

    elif message_type == "video":
        payload["interactive"] = {
            "type": "text",
            "header": {
                "type": "video",
                "video": {
                    "id": media_id
                }
            },
            "body": {"text": body} if body else None
        }

    elif message_type == "list_message":
        payload["interactive"] = {
            "type": "list",
            "header": {"type": "text", "text": header} if header else None,
            "body": {"text": body},
            "footer": {"text": footer} if footer else None,
            "action": {
                "button": "Choose an option",
                "sections": sections
            }
        }
    
    elif message_type == "reply_button_message":
        payload["interactive"] = {
            "type": "button",
            "body": {"text": body},
            "footer": {"text": footer} if footer else None,
            "action": {
                "buttons": button_data
            }
        }

    elif message_type == "single_product_message":
        payload["interactive"] = {
            "type": "product",
            "body": {"text": body},
            "footer": {"text": footer} if footer else None,
            "action": {
                "catalog_id": catalog_id,
                "product_retailer_id": product_data["product_retailer_id"]
            }
        }
    
    elif message_type == "multi_product_message":
        payload["interactive"] = {
            "type": "product_list",
            "header": {"type": "text", "text": header} if header else None,
            "body": {"text": body},
            "footer": {"text": footer} if footer else None,
            "action": {
                "catalog_id": catalog_id,
                "sections": sections
            }
        }
    
    elif message_type == "location_message":
        payload["type"] = "location"
        payload["location"] = {
            "latitude": latitude,
            "longitude": longitude,
            "name": header,
            "address": body
        }
    
    elif message_type == "location_request_message":
        payload["interactive"] = {
            "type": "LOCATION_REQUEST_MESSAGE",
            "body": {
                "text": body
            },
            "action": {
                "name": "send_location"
            }
        }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status != 200:
                    error_message = await response.text()
                    logger.error(f"Failed to send bot message to {contact}. Status: {response.status}, Error: {error_message}")
                    return
    except aiohttp.ClientError as e:
        logger.error(f"Error sending message to {contact}: {e}")
        return

async def send_messages(token: str,phone_number_id: str,template_name: str,language: str,media_type: str,media_id: ty.Optional[str],contact_list: ty.List[str],variable_list: ty.List[str],csv_variables: ty.Optional[ty.List[str]] = None,unique_id: str = "",request_id: Optional[str] = None) -> ty.List[ty.Any]:
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    results = []
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        # Determine batch iterator based on presence of csv_variables
        if csv_variables:
            batches = zip(chunks(contact_list, 78), chunks(csv_variables, 78))
        else:
            batches = ((batch, None) for batch in chunks(contact_list, 78))
            
        for contact_batch, variable_batch in batches:
            logger.info(f"Sending batch of {len(contact_batch)} contacts")
            logger.info(f"media_type {media_type}")
            
            if media_type == "OTP":
                send_func = send_otp_message
            else:
                send_func = send_message
                
            tasks = []
            for idx, contact in enumerate(contact_batch):
                csv_variable_list = variable_batch[idx] if variable_batch else None
                task = send_func(session=session,token=token,phone_number_id=phone_number_id,template_name=template_name,language=language,media_type="TEXT" if media_type == "OTP" else media_type,media_id=media_id,contact=contact,variables=variable_list,csv_variable_list=csv_variable_list)
                tasks.append(task)
            
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Error during batch processing: {e}", exc_info=True)
            
            # Rate limiting
            await asyncio.sleep(0.2)
    
    logger.info(f"All messages processed. Total results: {len(results)}")
    await notify_user(results, unique_id, request_id)
    
    return results

async def send_carousels(token: str, phone_number_id: str, template_name: str, contact_list: ty.List[str], media_id_list: ty.List[str], template_details: dict, unique_id: str, request_id: Optional[str] = None) -> None:
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    results = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(contact_list, 78):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [send_carousel(session, token, phone_number_id, template_name, contact, media_id_list, template_details) for contact in batch]
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Error during batch processing: {e}")
            await asyncio.sleep(0.2)
    await notify_user(results, unique_id, request_id)
    logger.info("All messages processed.")

    return results

async def send_template_with_flows(token: str, phone_number_id: str, template_name: str, flow_id: str, language: str, recipient_phone_number: ty.List[str],unique_id: str, request_id: Optional[str] = None) -> None:
    logger.info(f"Processing {len(recipient_phone_number)} contacts for sending messages.")
    results = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(recipient_phone_number, 78):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [send_template_with_flow(session, token, phone_number_id, template_name, flow_id, language, contact) for contact in batch]
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Error during batch processing: {e}")
            await asyncio.sleep(0.2)
    logger.info("All messages processed.")
    await notify_user(results, unique_id, request_id)

async def send_bot_messages(token: str, phone_number_id: str, contact_list: ty.List[str], message_type: str, header: ty.Optional[str] = None, body: ty.Optional[str] = None, footer: ty.Optional[str] = None, button_data: ty.Optional[ty.List[ty.Dict[str, str]]] = None, product_data: ty.Optional[ty.Dict] = None, catalog_id: ty.Optional[str] = None, sections: ty.Optional[ty.List[ty.Dict]] = None, latitude: ty.Optional[float] = None, longitude: ty.Optional[float] = None, media_id: ty.Optional[str] = None) -> None:
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(contact_list, 78):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [send_bot_message(session, token, phone_number_id, contact, message_type, header, body, footer, button_data, product_data, catalog_id, sections, latitude, longitude, media_id) for contact in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)
    logger.info("All messages processed.")

async def validate_numbers_async(token: str, phone_number_id: str, contact_list: ty.List[str], message_text: str, unique_id: str, report_id: Optional[str] = None) -> None:
    results = []
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(contact_list, 78):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [validate_nums(session, token, phone_number_id, contact, message_text) for contact in batch]
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Error during batch processing: {e}")
            await asyncio.sleep(0.2)
    
    # logger.info(f"All messages processed. Total results: {len(results)}")
    logger.info("Calling notify_user with results.")
    await notify_user(results, unique_id, report_id)

WEBHOOK_URL = "https://main.wtsmessage.xyz/notify_user/"

async def notify_user(results, unique_id: str, report_id):
    logger.info("notify_user function called")
    """Send results to a webhook as a notification when task is completed."""
    payload = {
        "status": "completed",
        "unique_id": unique_id,
        "report_id": report_id
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(WEBHOOK_URL, json=payload, headers=headers) as response:
                if response.status == 200:
                    logger.info("Successfully notified user via webhook.")
                else:
                    logger.error(f"Failed to notify user. Status: {response.status}, Response: {await response.text()}")
    except Exception as e:
        logger.error(f"Error notifying user via webhook: {e}")

def chunks(lst: ty.List[str], size: int) -> ty.Generator[ty.List[str], None, None]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
        

async def generate_media_id(file_path: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v17.0/{phone_id}/media"

    headers = {
        'Authorization': f'Bearer {token}'
    }

    try:
        # Create FormData object
        data = FormData()
        data.add_field('messaging_product', 'whatsapp')
        data.add_field('file', open(file_path, 'rb'), filename=os.path.basename(file_path), content_type='application/pdf')

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=data) as response:
                if response.status == 200:
                    media_id = (await response.json()).get('id')
                    logger.info(f"Media ID: {media_id}")
                    return media_id
                else:
                    error_text = await response.text()
                    logger.error(f"Error: {response.status} - {error_text}")
                    return None
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        return None

async def get_template_details_by_name(token: str, waba_id: str, template_name: str):
    url = f"https://graph.facebook.com/v14.0/{waba_id}/message_templates"
    
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params={"name": template_name}) as response:
                if response.status == 200:
                    templates = await response.json()
                    for template in templates.get('data', []):
                        if template['name'] == template_name:
                            return template
                    logging.error(f"Template with name {template_name} not found.")
                    raise HTTPException(status_code=404, detail=f"Template with name {template_name} not found.")
                else:
                    logging.error(f"Failed to get template details. Status code: {response.status}")
                    logging.error(f"Response: {await response.text()}")
                    raise HTTPException(status_code=response.status, detail="Failed to get template details.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post("/send_sms/")
async def send_messages_api(request: MessageRequest, background_tasks: BackgroundTasks):
    try:
        unique_id = generate_unique_id()
        
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
            unique_id=unique_id
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
async def send_sms_api(request: APIMessageRequest):
    # Step 1: Validate user credentials
    try:
        user_data = await fetch_user_data(request.user_id, request.api_token)
        logger.info(f"User validation successful for user_id: {request.user_id}")
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "510","status": "failed", "detail": e.detail}
    
    try:
        template = await get_template_details_by_name(user_data.register_app__token, user_data.whatsapp_business_account_id, request.template_name)
        category = template.get('category', 'Category not found')
    except HTTPException as e:
        logger.error(f"Template validation Failed: {e.detail}")
        return {"error code": "404 Not Found","status": "failed", "detail": e.detail} 
    
    # Step 2: Validate coins
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
        return {"error code": "520","status": "failed", "detail": e.detail}
    
    unique_id = generate_unique_id()
    
    # Step 3: Send messages 
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
            unique_id = unique_id
        )

        successful_sends = len([r for r in results if r['status'] == 'success'])
        failed_sends = len([r for r in results if r['status'] == 'failed'])
        
        # Step 4: Update balance and create report
        if successful_sends > 0:
            report_id = await update_balance_and_report(
                user_id=request.user_id,
                api_token=request.api_token,
                coins=successful_sends,  # Only deduct coins for successful sends
                contact_list=request.contact_list,
                template_name=request.template_name,
                category = category
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
async def send_sms_api(request: APIBalanceRequest):
    try:
        user_data = await fetch_user_data(request.user_id, request.api_token)
        logger.info(f"User validation successful for user_id: {request.user_id}")
        return {"balance": user_data.coins}
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "540","status": "failed", "detail": e.detail}
    
@app.post("/media_api/")
async def send_sms_api(
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

if __name__ == '__main__':
    logger.info("Starting the FastAPI server")
    uvicorn.run(app, host="127.0.0.1", port=8000)
