from utils import logger
import aiohttp
import typing as ty
import asyncio
from typing import Optional
from async_api_functions import send_otp_message, send_message, send_template_with_flow, notify_user, send_carousel, validate_nums, send_bot_message

def chunks(lst: ty.List[str], size: int) -> ty.Generator[ty.List[str], None, None]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]
        
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