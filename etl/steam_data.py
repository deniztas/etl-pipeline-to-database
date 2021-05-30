import os
import json
from typing import Any
import psycopg2

__location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

def database_connection() -> Any:
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        return cursor
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def users_data_stream():
    
    cur = database_connection()
    create_table_query = \
    """CREATE TABLE IF NOT EXISTS "Users"(\
    "userid" bigint PRIMARY KEY NOT NULL, \
    "location" VARCHAR(100) NOT NULL)"""
    cur.execute(create_table_query)
     
    # path = os.path.join(__location__, "etl", "raw_data", "users.ndjson")
    path = r"D:\Projects\etl-pipeline-to-database\etl\raw_data\users.ndjson"
    with open(path) as f:
        for line in f:
            j_content = json.loads(line)
            data_steam_query = \
                """INSERT INTO "Users"("userid","location") VALUES ({0}, '{1}')""" \
                    .format(j_content["USERID"], j_content["LOCATION"])
            cur.execute(data_steam_query)
    cur.close()

def jobs_data_stream():

    cur = database_connection()
    create_table_query = \
    """CREATE TABLE IF NOT EXISTS "Jobs"(\
    "jobidentifier" bigint PRIMARY KEY NOT NULL, \
    "servicename" VARCHAR(100) NOT NULL, \
    "userid" bigint NOT NULL, \
    "location" VARCHAR(100) NOT NULL, \
    "jobstatus" VARCHAR(50) NOT NULL, \
    "revenue" float NOT NULL, \
    CONSTRAINT userid FOREIGN KEY(userid) REFERENCES "Users"(userid))
    """
    cur.execute(create_table_query)

    # path = os.path.join(__location__, "etl", "raw_data", "jobs.ndjson")
    path = r"D:\Projects\etl-pipeline-to-database\etl\raw_data\jobs.ndjson"
    with open(path) as f:
        for line in f:
            j_content = json.loads(line)
            data_steam_query = \
                """INSERT INTO "Jobs" \
                ("jobidentifier","servicename", "userid", "location", "jobstatus", "revenue") \
                VALUES ({0}, '{1}', {2}, '{3}', '{4}', {5})""" \
                    .format(j_content["JOBIDENTIFIER"], j_content["SERVICENAME"],
                            j_content["USERID"], j_content["LOCATION"],
                            j_content["JOBSTATUS"], j_content["REVENUE"])
            cur.execute(data_steam_query)
    cur.close()

if __name__ == "__main__":
    users_data_stream()
    jobs_data_stream()