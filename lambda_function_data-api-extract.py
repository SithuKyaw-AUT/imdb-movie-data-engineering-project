import json
import os
import requests
import boto3
from datetime import datetime

def get_data(api_key):
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data:
            return data
        else: 
            print(f"Error: {data.get('status_message', 'unknown')}")
            return {}
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return {}

def lambda_handler(event, context):
    api_key = os.environ.get('api_key')
    
    raw_data = get_data(api_key)
    
    client = boto3.client('s3')
    
    filename = "imdb_movie_raw_data_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = "imdb-movies-data-project",
        Key = "raw_data/to_process/" + filename,
        Body = json.dumps(raw_data)
    )
    
    # if raw_data:
    #     return {
    #         'status_code': 200,
    #         'body': json.dumps(raw_data)
    #     }
    # else:
    #     return {
    #         'status_code': 500,
    #         'body': json.dumps('Failed to retrieve movie data')
    #     }

