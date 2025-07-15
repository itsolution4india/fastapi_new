import aiomysql
from typing import Optional

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