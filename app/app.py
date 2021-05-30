from flask import Flask, Response
from etl import steam_data
from typing import Any
import psycopg2
import json
#from sqlalchemy import create_engine


def database_connection() -> Any:

    # engine = create_engine('postgresql+psycopg2://postgres:admin@db/postgres')
    # connection = engine.raw_connection()
    # cursor = connection.cursor()
    # return cursor
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

app = Flask(__name__)

@app.route('/')
def start_etl():
    app.logger.info('request started')
    steam_data.users_data_stream()
    steam_data.jobs_data_stream()
    app.logger.info('request finished')
    return Response("ETL complated", mimetype='text/plain')


@app.route('/top_users/<loc>', methods=['GET'])
def top_users(loc):

    cur = database_connection()
    create_table_query = \
    """SELECT u.userid FROM "Users" AS u \
    INNER JOIN (SELECT * FROM "Jobs" WHERE location = '{0}') AS j \
    ON u.userid = j.userid AND u.location != j.location \
    ORDER BY REVENUE DESC \
    LIMIT 5""".format(loc)
    cur.execute(create_table_query)
    top_users = cur.fetchall()
    resp = Response(json.dumps(top_users))
    return resp

if __name__ == "__main__":
    app.run(debug=True)