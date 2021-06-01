import os
import json
from config import database_connection as db_conn


__location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

def users_data_stream():
    
    cur = db_conn()
    create_table_query = \
    """CREATE TABLE IF NOT EXISTS "Users"(\
    "userid" bigint PRIMARY KEY NOT NULL, \
    "location" VARCHAR(100) NOT NULL)"""
    cur.execute(create_table_query)
     
    path = os.path.join(__location__, "raw_data", "users.ndjson")

    with open(path) as f:
        for line in f:
            j_content = json.loads(line)
            data_steam_query = \
                """INSERT INTO "Users"("userid","location") VALUES ({0}, '{1}')""" \
                    .format(j_content["USERID"], j_content["LOCATION"])
            cur.execute(data_steam_query)

def jobs_data_stream():

    cur = db_conn()
    cur.execute("commit")
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

    path = os.path.join(__location__, "raw_data", "jobs.ndjson")

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