import pymysql
import pymssql
import os
import pandas as pd
import json

def GetGCP():
    current_directory = os.getcwd()
    return current_directory + "\\utils\\digital-bonfire-344816-9c1df57c7578.json"

def getMysqlData(qry,schema):
    # Connect to the database from json
    # Open and load the JSON file
    with open('utils\\digital-bdds.json', 'r') as json_file:
        data = json.load(json_file)

    connection = pymysql.connect(
        host=data['databases'][0]['connection_info']['host'],
        user=data['databases'][0]['connection_info']['user'],
        password=data['databases'][0]['connection_info']['password'],
        db=schema,
        charset="utf8mb4",
        )

    try:
        # Use Pandas to read data from MySQL into a DataFrame
        result = pd.read_sql(qry, connection)

    except Exception as e:
        print("Error:", str(e))

    finally:
        # Close the database connection when done
        connection.close()
        return result