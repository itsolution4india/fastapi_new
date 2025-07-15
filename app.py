from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db_models import Base, engine
from db_pool import init_mysql_pool, close_mysql_pool
from load_tracker import setup_load_tracking

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    
    await init_mysql_pool()
    try:
        yield
    finally:
        await close_mysql_pool()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_tracker = setup_load_tracking(app)