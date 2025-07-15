from contextlib import asynccontextmanager
from load_tracker import setup_load_tracking
from fastapi import FastAPI
from db_models import Base, engine
from fastapi.middleware.cors import CORSMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)

    from db_pool import init_mysql_pool, close_mysql_pool
    await init_mysql_pool()

    yield

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