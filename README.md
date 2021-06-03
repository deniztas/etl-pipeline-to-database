# etl-pipeline-to-database
## About Project
A mini-ETL (Extract Transform Load) pipeline that will move the Users and Jobs json files into a queryable PostgreSQL database. An HTTP endpoint that will require a job location as input, query the SQL database in step 1, and will return up to 5 User Ids as output.

## Getting Started

---

### Prerequisites
-  python 3.7.x
-  Docker

### Instructions
1. To build the application image execute this in the root directory of the project:
   ```$ docker-compose build```
3. Run the application besides database with:
    ```$ docker-compose up```
4.  You can reach the api from:
    ```http://localhost:5000/```

## How it works

+ As soon as the api container starts running, a new database is created if it doesn't exist and etl is started.
+ When ETL starts, firstly Users table created if it doesn't exist and loaded.
+ After Users table complated, loading of the Jobs table begins. Because UserId set as foreign key in Jobs table.
+ After ETL complated open a browser and go to (http://127.0.0.1:5000/) url you will see the a dropdown menu includes all user's locations
+ There is a http endpoint that get a loc parameter and return top 5 user.For instance, if you select ```LOC_1886``` from dropdown menu you will redirected to** (http://127.0.0.1:5000/top_users?loc=LOC_1886) url, http endpoint returns ```[98770174388, 98770178990, 98770149866, 98770178955, 98770176093]``` list
