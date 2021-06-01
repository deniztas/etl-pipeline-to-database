import os
import psycopg2
from typing import Any
from sqlalchemy import create_engine


db_url = os.environ["SQLALCHEMY_DATABASE_URI"]
db_name = db_url.split("/")[-1]
__engine__ = create_engine(db_url)

def create_db():
    cursor = database_connection()
    cursor.execute("commit")
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}'".format(db_name))
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('CREATE DATABASE {}'.format(db_name))

def database_connection() -> Any:
    try:
        connection = __engine__.raw_connection()
        cursor = connection.cursor()
        cursor.execute("commit")
        return cursor
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
