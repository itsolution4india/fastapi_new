from sqlalchemy import Column, Integer, String, Text, Date, BigInteger, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
from typing import List, Optional
import re


DATABASE_URL = "postgresql://postgres:Solution@97@localhost:5432/Admin"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ReportInfo(Base):
    __tablename__ = "smsapp_reportinfo"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.now)
    email = Column(String(100))
    campaign_title = Column(String(50))
    contact_list = Column(Text)
    waba_id_list = Column(Text, default="0")
    message_date = Column(Date)
    template_name = Column(String(100))
    message_delivery = Column(BigInteger)
    start_request_id = Column(BigInteger, default=0)
    end_request_id = Column(BigInteger, default=0)