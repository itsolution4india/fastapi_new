import logging
import typing as ty
import aiofiles
from pydantic import BaseModel
from typing import Optional, List

            
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