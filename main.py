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

# Configure logging to log to console only (no file).
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbose logging
    format="%(asctime)s [%(levelname)s] %(message)s",  # Include timestamp
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler()  # Log to console (stdout)
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI()

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

class UserData(BaseModel):
    whatsapp_business_account_id: str
    phone_number_id: str
    register_app__app_id: str
    register_app__token: str
    coins: int

class APIBalanceRequest(BaseModel):
    user_id: str
    api_token: str

class UpdateBalanceReportRequest(BaseModel):
    user_id: str
    api_token: str
    coins: int
    phone_numbers: str
    all_contact: ty.List[int]
    template_name: str

async def fetch_user_data(user_id: str, api_token: str) -> UserData:
    """Fetch user data from the API"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://wtsdealnow.com/api/users/")
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
                coins=user["coins"]
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
    template_name: str
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
            template_name=template_name
        )
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://wtsdealnow.com/update-balance-report/",
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


class MessageRequest(BaseModel):
    token: str
    phone_number_id: str
    template_name: str
    language: str
    media_type: str
    media_id: ty.Optional[str]
    contact_list: ty.List[str]
    variable_list: ty.Optional[ty.List[str]] = None

class FlowMessageRequest(BaseModel):
    token: str
    phone_number_id: str
    template_name: str
    flow_id: str
    language: str
    recipient_phone_number: ty.List[str]

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

async def send_message(session: aiohttp.ClientSession, token: str, phone_number_id: str, template_name: str, language: str, media_type: str, media_id: ty.Optional[str], contact: str, variables: ty.Optional[ty.List[str]] = None) -> None:
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

async def send_otp_message(session: aiohttp.ClientSession, token: str, phone_number_id: str, template_name: str, language: str, media_type: str, media_id: ty.Optional[str], contact: str, variables: ty.Optional[ty.List[str]] = None) -> None:
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
        "text": "123456"
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

async def send_messages(token: str, phone_number_id: str, template_name: str, language: str, media_type: str, media_id: ty.Optional[str], contact_list: ty.List[str], variable_list: ty.List[str]) -> None:
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    results = []
    if media_type == "OTP":
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
            for batch in chunks(contact_list, 75):
                logger.info(f"Sending batch of {len(batch)} contacts")
                tasks = [send_otp_message(session, token, phone_number_id, template_name, language, "TEXT", media_id, contact, variable_list) for contact in batch]
                batch_results = await asyncio.gather(*tasks)
                results.extend(batch_results)
                await asyncio.sleep(0.5)
    else:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
            for batch in chunks(contact_list, 75):
                logger.info(f"Sending batch of {len(batch)} contacts")
                tasks = [send_message(session, token, phone_number_id, template_name, language, media_type, media_id, contact, variable_list) for contact in batch]
                batch_results = await asyncio.gather(*tasks)
                results.extend(batch_results)
                await asyncio.sleep(0.5)
        logger.info("All messages processed.")

    return results

async def send_template_with_flows(token: str, phone_number_id: str, template_name: str, flow_id: str, language: str, recipient_phone_number: ty.List[str]) -> None:
    logger.info(f"Processing {len(recipient_phone_number)} contacts for sending messages.")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(recipient_phone_number, 75):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [send_template_with_flow(session, token, phone_number_id, template_name, flow_id, language, contact) for contact in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.5)
    logger.info("All messages processed.")

async def send_bot_messages(token: str, phone_number_id: str, contact_list: ty.List[str], message_type: str, header: ty.Optional[str] = None, body: ty.Optional[str] = None, footer: ty.Optional[str] = None, button_data: ty.Optional[ty.List[ty.Dict[str, str]]] = None, product_data: ty.Optional[ty.Dict] = None, catalog_id: ty.Optional[str] = None, sections: ty.Optional[ty.List[ty.Dict]] = None, latitude: ty.Optional[float] = None, longitude: ty.Optional[float] = None, media_id: ty.Optional[str] = None) -> None:
    logger.info(f"Processing {len(contact_list)} contacts for sending messages.")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1000)) as session:
        for batch in chunks(contact_list, 75):
            logger.info(f"Sending batch of {len(batch)} contacts")
            tasks = [send_bot_message(session, token, phone_number_id, contact, message_type, header, body, footer, button_data, product_data, catalog_id, sections, latitude, longitude, media_id) for contact in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.5)
    logger.info("All messages processed.")

def chunks(lst: ty.List[str], size: int) -> ty.Generator[ty.List[str], None, None]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

@app.post("/send_sms/")
async def send_messages_api(request: MessageRequest):
    try:
        await send_messages(
            token=request.token,
            phone_number_id=request.phone_number_id,
            template_name=request.template_name,
            language=request.language,
            media_type=request.media_type,
            media_id=request.media_id,
            contact_list=request.contact_list,
            variable_list=request.variable_list
        )
        return {'message': 'Messages sent successfully'}
    except HTTPException as e:
        logger.error(f"HTTP error: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {e}")

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
async def send_flow_message_api(request: FlowMessageRequest):
    try:
        status_code, response_dict = await send_template_with_flows(
            request.token,
            request.phone_number_id,
            request.template_name,
            request.flow_id,
            request.language,
            request.recipient_phone_number
        )
        if status_code == 200:
            return {'message': 'Flow message sent successfully', 'response': response_dict}
        else:
            logger.error(f"Failed to send flow message. Status code: {status_code}, Response: {response_dict}")
            raise HTTPException(status_code=status_code, detail=f"Error sending flow message: {response_dict}")
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
    
    # Step 2: Validate coins
    try:
        total_contacts = len(request.contact_list)
        await validate_coins(user_data.coins, total_contacts)
        logger.info(f"Coin validation successful. Required: {total_contacts}, Available: {user_data.coins}")
    except HTTPException as e:
        logger.error(f"Coin validation failed: {e.detail}")
        return {"error code": "520","status": "failed", "detail": e.detail}
    
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
            variable_list=request.variable_list
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
                template_name=request.template_name
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

@app.post("/balance_check_api/")
async def send_sms_api(request: APIBalanceRequest):
    try:
        user_data = await fetch_user_data(request.user_id, request.api_token)
        logger.info(f"User validation successful for user_id: {request.user_id}")
        return {"balance": user_data.coins}
    except HTTPException as e:
        logger.error(f"User validation failed: {e.detail}")
        return {"error code": "540","status": "failed", "detail": e.detail}

    

if __name__ == '__main__':
    logger.info("Starting the FastAPI server")
    uvicorn.run(app, host="127.0.0.1", port=8000)
