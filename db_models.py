from sqlalchemy import Column, Integer, String, Text, Date, BigInteger, DateTime, create_engine, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
from typing import List, Optional
import re
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship


DATABASE_URL = "postgresql://postgres:Solution%4097@217.145.69.172:5432/adminmain"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ReportInfo(Base):
    __tablename__ = "smsapp_reportinfo"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    email = Column(String(100))
    campaign_title = Column(String(50))
    contact_list = Column(Text)
    waba_id_list = Column(Text, default="0")
    message_date = Column(Date)
    template_name = Column(String(100))
    message_delivery = Column(BigInteger)
    start_request_id = Column(String(50), default='0')
    end_request_id = Column(String(50), default='0')
    deliver_count = Column(Integer, default=0, nullable=False)
    sent_count = Column(Integer, default=0, nullable=False)
    read_count = Column(Integer, default=0, nullable=False)
    pending_count = Column(Integer, default=0, nullable=False)
    failed_count = Column(Integer, default=0, nullable=False)
    reply_count = Column(Integer, default=0, nullable=False)
    total_count = Column(Integer, default=0, nullable=False)

class User(Base):
    __tablename__ = "smsapp_customuser"  # make sure this matches your DB table name

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    username = Column(String, unique=True)
    phone_number_id = Column(String)
    whatsapp_business_account_id = Column(String)
    coins = Column(Integer, default=0)
    marketing_coins = Column(Integer, default=0)
    authentication_coins = Column(Integer, default=0)
    discount = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    is_staff = Column(Boolean, default=False)
    user_id = Column(String, default="0")
    api_token = Column(Text, default="0")
    remarks = Column(String, default="0")

    # Reverse relationship
    whitelist_entries = relationship("WhitelistBlacklist", back_populates="email")
    
class WhitelistBlacklist(Base):
    __tablename__ = "smsapp_whitelist_blacklist"

    id = Column(Integer, primary_key=True, index=True)
    email_id = Column(Integer, ForeignKey("smsapp_customuser.id"), nullable=False)
    whitelist_phone = Column(Text)
    blacklist_phone = Column(Text)

    email = relationship("User", back_populates="whitelist_entries")