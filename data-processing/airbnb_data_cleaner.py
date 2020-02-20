import io
from io import BytesIO
import os
import boto3
import pandas as pd
import s3fs
import numpy as np
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('bnbpipe')
s3_path = 's3://bnbpipe/'

### Get list of files to access from S3 bucket
year_to_skip = 2015 ### skipping 2015 as it lacks info on number of reviews 
year_file_list = []
for my_bucket_object in my_bucket.objects.all():
    #print("All Files:",my_bucket_object.key)
    current_file = my_bucket_object.key
    current_list = current_file.split('__')
    current_city = current_list[0]
    current_year = int(current_list[1])
    current_month = int(current_list[2])
    if current_year == year_to_skip:
        continue
    year_file_list.append(my_bucket_object.key)


cleaned_bucket = s3.Bucket('bnbcleanedv3')
s3_path_cleaned = 's3://bnbcleanedv3/'
for file_names in year_file_list:
    print("Trying to upload:",file_names)
    current_file = file_names
    current_list = current_file.split('__')
    current_city = current_list[0]
    current_year = int(current_list[1])
    current_month = int(current_list[2])
    ### Extracting only the required columns
    df = pd.read_csv(os.path.join(s3_path,file_names),usecols =["id","listing_url","host_id","host_url","host_name","city","state","country","price","number_of_reviews","review_scores_rating","calculated_host_listings_count","reviews_per_month","Year","Month"]) 
    ### Data quality check - filter out listings with no valid ID, price, reviews per month
    df = df[np.isfinite(df['reviews_per_month'])]
    df[df['id'].str.strip().astype(bool)]
    df[df['price'].str.strip().astype(bool)]

    df['city_name'] = current_city
    df['Year'] = current_year
    df['Month'] = current_month
    
    #Writing to S3 using BOTO3
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer)
    s3_cleaned = boto3.resource('s3')
    s3.Object('bnbcleanedv3',file_names).put(Body=csv_buffer.getvalue())


