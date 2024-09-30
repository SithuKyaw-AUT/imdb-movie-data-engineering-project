import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
import csv 

def clean_genre_columns(df):
    # Convert genre_ids and genre_names lists into comma-separated strings
    df['genre_ids'] = df['genre_ids'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    df['genre_names'] = df['genre_names'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Remove any extra quotes that might have been introduced
    df = df.replace({r'"': ''}, regex=True)

    return df
    
def transform_all_movies_data(raw_data_list):
    all_movies_data = []
    genre_map = {28: 'Action',
                 12: 'Adventure',
                 16: 'Animation',
                 35: 'Comedy',
                 80: 'Crime',
                 99: 'Documentary',
                 18: 'Drama',
                 10751: 'Family',
                 14: 'Fantasy',
                 36: 'History',
                 27: 'Horror',
                 10402: 'Music',
                 9648: 'Mystery',
                 10749: 'Romance',
                 878: 'Science Fiction',
                 10770: 'TV Movie',
                 53: 'Thriller',
                 10752: 'War',
                 37: 'Western'}

    for raw_data in raw_data_list:
        if 'results' in raw_data:
            for row in raw_data['results']:
                adult = row['adult']
                genre_ids = row['genre_ids']
                genre_names = [genre_map.get(genre_id, "unknown") for genre_id in genre_ids]
                
                # Convert lists to comma-separated strings
                genre_ids_str = ', '.join(map(str, genre_ids))
                genre_names_str = ', '.join(genre_names)
                
                movie_id = row['id']
                language = row['original_language']
                title = row['title']
                overview = row['overview']
                popularity = row['popularity']
                release_date = row['release_date']
                vote_average = row['vote_average']
                vote_count = row['vote_count']
                movie_element = {'movie_id': movie_id, 'title': title, 'genre_ids': genre_ids_str, 'genre_names': genre_names_str,
                                 'release_date': release_date, 'language': language, 'adult': adult, 'overview': overview, 
                                 'popularity': popularity, 'vote_average': vote_average, 'vote_count': vote_count}
                all_movies_data.append(movie_element)

        else: 
            print(f"There is no movie data in the file")
    
    return all_movies_data
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "imdb-movies-data-project"
    Key = "raw_data/to_process/"
    
    imdb_data = []
    imdb_key = []
    
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            imdb_data.append(jsonObject)
            imdb_key.append(file_key)

    transform_data = transform_all_movies_data(imdb_data)
    
    transform_data_df = pd.DataFrame(transform_data)
    
    transform_data_df['release_date'] = pd.to_datetime(transform_data_df['release_date']) 
    
    # Use the cleaning function before writing to CSV
    transform_data_df = clean_genre_columns(transform_data_df)
    
    filename = "transformed_data/transformed_" + str(datetime.now()) + ".csv"
    
    data_buffer = StringIO()
    transform_data_df.to_csv(data_buffer, index=False, quoting=csv.QUOTE_ALL)
    transform_data_df_content = data_buffer.getvalue()
    
    s3.put_object(
        Bucket = Bucket,
        Key = filename,
        Body = transform_data_df_content
    )
    
    s3_resource = boto3.resource('s3')
    
    for key in imdb_key:
        copy_source = {
            'Bucket':Bucket,
            'Key':key
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
    