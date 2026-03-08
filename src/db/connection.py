import psycopg2

from src.utils.config import DB


def get_connection():
    return psycopg2.connect(
        host=DB.host,
        port=DB.port,
        dbname=DB.name,
        user=DB.user,
        password=DB.password,
    )
