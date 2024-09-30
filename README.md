# IMDb Movie Data End-to-end Data Engineering Project
### Introduction
This project builds an ETL pipeline by using AWS. I used TMDb api to extract the movie data. The pipeline retrieves data from the TMDb api, transform it to the desired format, and load it to the AWS S3 storage. 
The data is crawled to Data Catalog and analysed by Athena.

### Architecture
![Architecture Diagram](https://github.com/SithuKyaw-AUT/imdb-movie-data-engineering-project/blob/main/tmdb-data-engineering-project.png)

### Project Flow
1. Extract data from API
2. Lambda function is triggered every 15 minutes to extract data and save the raw data in S3 bucket.
3. Lambda function is triggered when the raw_data bucket is updated by a json file. The function transforms the data to desired format and save the transformed data in the S3 bucket.
4. Glue Crawler is run to load the transformed data table into Data Catalog.
5. Athena is used to analyse data with the interactive queries. 

### TMDb API
The [api](https://developer.themoviedb.org/reference/intro/getting-started) contains information about movie data. 

### AWS Services 
+ S3 (Simple Storage Service)
+ Lambda 
+ Cloud Watch
+ Glue Crawler
+ Data Catalog
+ Athena


