# airflow_aws_justwatch_pipeline
Data pipeline using Airflow, GraphQL, AWS S3, AWS Glue Jobs and AWS Redshift.
The objective is to create a database in AWS Redshift with data from [JustWatch.com](https://www.justwatch.com/), a website from where is possible to watch movies and shows provided by many streaming services (Netflix, Youtube, Amazon Prime and many others).
But for this project I just considered the 10 most watched services:
- Amazon Prime Video
- Apple TV Plus
- Crunchyroll
- Disney Plus
- Hulu
- Netflix
- Paramount Plus
- Peacock
- Tubi TV
- YouTube

The pipeline will follow the steps below:
1. Extract data from endpoint API 'https://apis.justwatch.com/graphql'
2. Save the raw data into JSON files.
3. Upload JSON files to AWS S3 bucket.
4. Trigger AWS Glue Jobs that process the titles data to normalized data into parquet files.
    Example: 1 title has many production countries in the raw data. 
            The Glue Job separates all these productions countries from raw data 
            and transform them into parquets that will be read from a Redshift database.
5. Transform these parquet files into a Redshift database.
   
