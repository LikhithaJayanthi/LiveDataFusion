
import time
from dotenv import load_dotenv,find_dotenv
import requests
import pandas as pd
from pymongo import MongoClient
import os
from urllib.parse import quote_plus
load_dotenv(find_dotenv())
from bson import json_util


Password = os.environ.get("mongodb_pwd")
escaped_password = quote_plus(Password)


connection_string=f"mongodb+srv://farsim:{escaped_password}@mycluster.thlwhor.mongodb.net/?retryWrites=true&w=majority" 
client =MongoClient(connection_string)
# MongoDB connection
db = client['kafka_stream']
collection = db.kafka_data

# Power BI streaming dataset URL and API key,replace the api key
api_url = 'https://api.powerbi.com/beta/b6420cd0-1f73-4471-9a39-20953822a34a/datasets/fe7cf224-90d6-40d2-99b2-76a3193336c7/rows?experience=power-bi&key=qySokGJxbxSsoGDGuclE4lKKTXXegDImsUISw5MpT4j3xzEnA%2FcE5E0BGZGU%2FLJk66j3UyrRF6ZGtB5tEL3CvA%3D%3D'
while True:
    # Fetch data from MongoDB
    data = collection.find().sort('_id', -1).limit(10)
    df = pd.DataFrame(data)
    df['Datetime'] = pd.to_datetime(df['Datetime']).apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

    # df['Datetime']= df['Datetime'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        # Convert DataFrame to a list of dictionaries
    records = json_util.loads(df.to_json(orient='records', default_handler=str))

    # Manually convert ObjectId to str
    for record in records:
        record['_id'] = str(record['_id'])
        print(record)
        # print (records)
    # Push data to Power BI streaming dataset
    try:
        response = requests.post(api_url, json=records)

        print(f'Status Code: {response.status_code}')
    except Exception as e:
        print(f'Error: {e}')

    time.sleep(1)  # Wait for 10 seconds before the next iteration
