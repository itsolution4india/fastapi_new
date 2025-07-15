import aiomysql
from contextlib import asynccontextmanager
from fastapi import HTTPException
from typing import Optional
import asyncio

mysql_pool: Optional[aiomysql.Pool] = None

dbconfig = {
    "host": "localhost",
    "port": 3306,
    "user": "prashanth@itsolution4india.com",
    "password": "Solution@97",
    "db": "webhook_responses",
    "charset": "utf8mb4",
}

async def init_db_pool():
    global connection_pool
    connection_pool = await aiomysql.create_pool(
        minsize=10,
        maxsize=50,
        pool_recycle=3600,
        autocommit=True,
        **dbconfig
    )

async def close_db_pool():
    global connection_pool
    if connection_pool:
        connection_pool.close()
        await connection_pool.wait_closed()
        
@asynccontextmanager
async def get_db_connection(retries: int = 2):
    from .db_pool import connection_pool
    if not connection_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")

    for attempt in range(retries):
        try:
            async with connection_pool.acquire() as conn:
                yield conn
                return
        except Exception as e:
            if attempt + 1 >= retries:
                raise HTTPException(status_code=500, detail="Database connection error")
            await asyncio.sleep(1)