import aiomysql
from typing import Optional

mysql_pool: Optional[aiomysql.Pool] = None

async def init_mysql_pool():
    global mysql_pool
    mysql_pool = await aiomysql.create_pool(
        host="localhost",
        port=3306,
        user="prashanth@itsolution4india.com",
        password="Solution@97",
        db="webhook_responses",
        charset="utf8mb4",
        autocommit=True,
        minsize=5,
        maxsize=20
    )

async def close_mysql_pool():
    global mysql_pool
    if mysql_pool:
        mysql_pool.close()
        await mysql_pool.wait_closed()