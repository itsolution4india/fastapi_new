import logging
import typing as ty
import aiohttp
import json
from fastapi import HTTPException
import httpx
import httpx
from .models import UserData
from .utils import logger
from models import UpdateBalanceReportRequest
from aiohttp import FormData
import os

WEBHOOK_URL = "https://wtsdealnow.com/notify_user/"

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
            status_code=402,
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