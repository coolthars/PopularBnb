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

#renter_year = 2019
city_to_skip = "oakland"
year_to_skip = 2015
start_flag = 0
year_file_list = []
for my_bucket_object in my_bucket.objects.all():
    #print("All Files:",my_bucket_object.key)
    current_file = my_bucket_object.key
    current_list = current_file.split('__')
    current_city = current_list[0]
    current_year = int(current_list[1])
    current_month = int(current_list[2])
    #year_file_list.append(my_bucket_object.key)
    if current_city == city_to_skip:
        if current_year == year_to_skip:
            start_flag = 1
            continue
    if current_year == year_to_skip:
        continue
    if start_flag == 1:
        year_file_list.append(my_bucket_object.key)
    #if current_year == renter_year:
        #if current_city == "amsterdam":
            #print("Parse File:",current_file)
            #year_file_list.append(my_bucket_object.key)

current_year_df = []

cleaned_bucket = s3.Bucket('bnbpipecleaned')
s3_path_cleaned = 's3://bnbpipecleaned/'
for file_names in year_file_list:
    print("Trying to upload:",file_names)
    #df = pd.read_csv(os.path.join(s3_path,file_names),usecols =["id","listing_url","price","Year","Month","number_of_reviews","reviews_per_month"])
    #df = pd.read_csv(os.path.join(s3_path,file_names),usecols =["id","listing_url","host_id","host_url","host_name","host_since","city","state","country_code","country","price","number_of_reviews","number_of_reviews_ltm","review_scores_rating","review_scores_accuracy","review_scores_cleanliness","review_scores_checkin","review_scores_communication","review_scores_location","review_scores_value","calculated_host_listings_count","reviews_per_month","Year","Month"]) 
    df = pd.read_csv(os.path.join(s3_path,file_names),usecols =["id","listing_url","host_id","host_url","host_name","host_since","city","state","country","price","number_of_reviews","review_scores_rating","calculated_host_listings_count","reviews_per_month","Year","Month"]) 
    df = df[np.isfinite(df['reviews_per_month'])]
    
    #Testing locally 
    #export_csv = df.to_csv(file_names,index = None, header = True)
    #print(df)
    #my_bc = str(cleaned_bucket)
    #s3_client.upload_file(file_names,'bnbpipecleaned',file_names)
    
    #Writing to S3 using S3FS
    #bytes_to_write = df.to_csv(None).encode
    #fs = s3fs.S3FileSystem(key=key, secret=secret)
    #fs = s3fs.S3FileSystem(anon=False)
    #with fs.open(os.path.join(s3_path_cleaned,file_names),'wb') as f:
    #    f.write(bytes_to_write)

    #Writing to S3 using BOTO3
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer)
    s3_cleaned = boto3.resource('s3')
    s3.Object('bnbpipecleaned',file_names).put(Body=csv_buffer.getvalue())
    current_year_df.append(df)

#concatenated_df = pd.concat(current_year_df, axis = 0, ignore_index=True)

#for index, row in concatenated_df.iterrows():
#    print(row['Year'], row['Month'], row['id'], row['price'], row['number_of_reviews'],row['reviews_per_month'],row['listing_url'])


#df = pd.read_csv('s3://bnbpipe/amsterdam__2015__04__listings.csv',usecols =["id","listing_url","price","Year","Month","number_of_reviews","reviews_per_month"])
#df = df[np.isfinite(df['reviews_per_month'])]

#obj = client.get_object(Bucket=my_bucket,Key='amsterdam__2015__04__listings.csv')
#df = pd.read_csv(io.BytesIO(obj['Body'].read()),encoding='utf8')
#df = pd.read_csv("amsterdam__2015__04__listings.csv", usecols =["id","listing_url","price","Year","Month"])

#for index, row in df.iterrows():
#    print(row['Year'], row['Month'], row['id'], row['price'], row['number_of_reviews'],row['reviews_per_month'],row['listing_url']) 


#print("LIST of all Files")

#files = list(my_bucket.objects)
#files = client.list_objects()

#for my_bucket_object in my_bucket.objects.all():
#    print(my_bucket_object)
