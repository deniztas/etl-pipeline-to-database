import os
import psycopg2
from typing import Any
from sqlalchemy import create_engine


__engine__ = create_engine(os.environ["SQLALCHEMY_DATABASE_URI"])

def create_db():
    cursor = database_connection()
    cursor.execute("commit")
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'postgres'")
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('CREATE DATABASE postgres')

def database_connection() -> Any:
    try:
        connection = __engine__.raw_connection()
        cursor = connection.cursor()
        cursor.execute("commit")
        return cursor
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
