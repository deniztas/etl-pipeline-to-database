from flask import Flask, Response, request
from etl import steam_data
import json
from config import database_connection as db_conn, create_db


app = Flask(__name__)

@app.before_first_request
def create_database():
    create_db()
    return Response("Database created", mimetype='text/plain')

@app.route('/etl')
def start_etl():
    app.logger.info('request started')
    steam_data.users_data_stream()
    steam_data.jobs_data_stream()
    app.logger.info('request finished')
    return Response("ETL complated", mimetype='text/plain')


@app.route('/top_users', methods=['GET'])
def top_users():

    cur = db_conn()
    loc = request.args['loc']
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