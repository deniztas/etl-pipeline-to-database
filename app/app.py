from flask import Flask, Response, request, render_template
from etl import steam_data
import json
from config import get_db_connection as db_conn, create_db


# Create db and etl before api started

create_db()
steam_data.start_etl()

app = Flask(__name__, template_folder='template')


@app.route('/')
def index():
    """Home page"""

    # Create a cursor
    cur = db_conn()
    # Execute select statement to fetch data to be displayed in dropdown
    distinct_loc_query = """\
        SELECT DISTINCT location FROM "Users" \
        ORDER BY location"""
    cur.execute(distinct_loc_query)
    location_list = cur.fetchall()
    return render_template("index.html", location_list=location_list)


@app.route('/top_users', methods=['GET'])
def top_users():
    """Api that get top 5 users according
    to selected location"""

    loc = request.args['loc']
    top_users = get_top_users(loc)
    resp = Response(json.dumps(top_users))
    return resp


def get_top_users(loc):
    """Execute postgres query and return top 5 userid"""

    cur = db_conn()
    create_table_query = """SELECT u.userid FROM "Users" AS u \
    INNER JOIN (SELECT * FROM "Jobs" WHERE location = '{0}') AS j \
    ON u.userid = j.userid AND u.location != j.location \
    WHERE revenue > 0 \
    ORDER BY REVENUE DESC \
    LIMIT 5""".format(loc)
    cur.execute(create_table_query)
    return cur.fetchall()


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
