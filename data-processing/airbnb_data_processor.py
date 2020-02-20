import sys
import io
from io import BytesIO
import os
import boto3
import pandas as pd
import glob
import s3fs
import csv
import numpy as np
import datetime
import shutil
from pyspark import SparkConf                                                                                                                 
from pyspark.context import SparkContext                                                                                                      
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.appName("Py").getOrCreate()

def clean_currency(x):
    """ If the value is a string, then remove currency symbol and delimiters
    otherwise, the value is numeric and can be converted
    """
    if isinstance(x, str):
        return(x.replace('$', '').replace(',', ''))
    return(x)


### Read all CSVs for a given year into Spark DataFrames - get per Calendar Month price for each Listing


def bnb_processor(city1):
    ### Get listings files for a given city between 2017 - 2019 
    print("Running AirBNB Data Processor for City:",city1)
    summary_year = 2019
    #path = "s3a://bnbcleanedv2/london*2019*"
    path = 's3a://bnbcleanedv2/' + city1 + '*{2017,2018,2019}*'

    ### Defining data schema 
    cleaned_df = spark.read.format('com.databricks.spark.csv')\
            .options(header='true')\
            .options(delimiter=',')\
            .options(quote='"')\
            .options(escape='"')\
            .options(multiline='true')\
            .load(path)
    #["id","listing_url","host_id","host_url","host_name","city_name","city","state","country","price","number_of_reviews","calculated_host_listings_count","reviews_per_month","Year","Month"])
    cleaned_df = cleaned_df.withColumn('id',cleaned_df.id.cast('INT'))
    cleaned_df = cleaned_df.withColumn('host_id',cleaned_df.host_id.cast('INT'))
    cleaned_df = cleaned_df.withColumn('price',cleaned_df.price.cast('STRING'))
    cleaned_df = cleaned_df.withColumn('number_of_reviews',cleaned_df.number_of_reviews.cast('INT'))
    cleaned_df = cleaned_df.withColumn('calculated_host_listings_count',cleaned_df.calculated_host_listings_count.cast('INT'))
    cleaned_df = cleaned_df.withColumn('Year',cleaned_df.Year.cast('INT'))
    cleaned_df = cleaned_df.withColumn('Month',cleaned_df.Month.cast('INT'))

    row_len = df.count()
    col_len = len(df.columns)
    print("Rows: ",row_len,"Columns: ",col_len)
 

    cleaned_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydataallcsv")
    all_files = glob.glob("mydataallcsv/*csv")

    
    df = []
    for file_name in all_files:
        print("Info: Parsing File",file_name)
        df_tmp = pd.read_csv(file_name,usecols =["id","listing_url","host_id","host_url","host_name","city_name","city","state","country","price","number_of_reviews","calculated_host_listings_count","reviews_per_month","Year","Month"])
        df_tmp['price'] = df_tmp['price'].apply(clean_currency).astype('float')
        df_tmp = df_tmp[pd.notnull(df_tmp['price'])]
        df.append(df_tmp)
        #df_tmp.dtypes
        #df_tmp.info()
 
    concat_df = pd.concat(df, axis = 0, ignore_index=True)

    counti = 0
    #start_year = concat_df['Year'].min()
    #end_year = concat_df['Year'].max()
    start_year = 2017
    end_year = 2019
    airbnb_listing_dict = {}
    airbnb_investor = {}
    info_list = ['country','state','city','listing_url','host_id','host_url','calculated_host_listings_count','number_of_reviews']
    print("BNB INFO: Populating Dict")

    ### Creating dict for each city Renters and Investors data 
    for index,row in concat_df.iterrows():
        counti += 1
        if row['city_name'] not in airbnb_listing_dict:
            airbnb_listing_dict[row['city_name']] = {}
        if row['id'] not in airbnb_listing_dict[row['city_name']]:
            airbnb_listing_dict[row['city_name']][row['id']] = {}
            airbnb_listing_dict[row['city_name']][row['id']]['per_year'] = {}
            airbnb_listing_dict[row['city_name']][row['id']]['info'] = {}
            ### Populate All info parameters we care about
            for info_param in info_list:
                if info_param == "number_of_reviews":
                    if info_param not in airbnb_listing_dict[row['city_name']][row['id']]['info']:
                        airbnb_listing_dict[row['city_name']][row['id']]['info'][info_param] = 0
                    if ( pd.notnull(row[info_param])):
                        if airbnb_listing_dict[row['city_name']][row['id']]['info'][info_param] < row[info_param]:
                            airbnb_listing_dict[row['city_name']][row['id']]['info'][info_param] = row[info_param]
                else:
                    airbnb_listing_dict[row['city_name']][row['id']]['info'][info_param] = row[info_param] 
        if row['Year'] not in airbnb_listing_dict[row['city_name']][row['id']]['per_year']:
            airbnb_listing_dict[row['city_name']][row['id']]['per_year'][row['Year']] = {}
        try:
            airbnb_listing_dict[row['city_name']][row['id']]['per_year'][row['Year']][int(row['Month'])] = row['price']
        except:
            print(counti,row['city_name'],row['id'],row['Year'],row['Month'])        

    ### For renters summary - we are creating the summary only for previous year         
    now = datetime.datetime.now()
    last_year = now.year - 1

    print ("BNB INFO: Computing Monthly Averages and Printing")
    renter_out = city1+"__renter_summary.csv"
    investor_out = city1+"__investor_summary.csv"

    with open(renter_out,'w') as out, open(investor_out,'w') as inv_out:
        csv_out = csv.writer(out)
        csv_out_inv = csv.writer(inv_out)
        ### printing headers for renters summary 
        csv_out.writerow(['city_name','listing_id','country','state','city','listing_url','host_id','host_url','calculated_host_listings_count','number_of_reviews','Year','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','AVG','MIN','MAX','Price_Variation']);
        year1 = start_year

        ### printing header for investors summary 
        print_inv_header = []
        print_inv_header.append("city_name")
        while ( year1 <= end_year):
            l_count = str(year1) + "__listings_count"
            l_avg = str(year1) + "__average_price"
            print_inv_header.append(l_count)
            print_inv_header.append(l_avg)
            year1 += 1
        csv_out_inv.writerow(print_inv_header)


        for city_name in sorted(airbnb_listing_dict):
            if city_name not in airbnb_investor:
                airbnb_investor[city_name] = {}
            for listing_id in sorted(airbnb_listing_dict[city_name]):
                for year in airbnb_listing_dict[city_name][listing_id]['per_year']:
                    if year not in airbnb_investor[city_name]:
                        airbnb_investor[city_name][year] = {}
                        airbnb_investor[city_name][year]['listing_list'] = []
                        airbnb_investor[city_name][year]['listing_avg_list'] = []
                        print("Initializing Investor Dict",city_name,year)
                    print_list_renter = []
                    print_list_renter.append(city_name)
                    print_list_renter.append(listing_id)
                    for info_param in info_list:
                        print_list_renter.append(airbnb_listing_dict[city_name][listing_id]['info'][info_param])
                    print_list_renter.append(year)
                    monthly_price = []
                    my_nan_count = 0
                    for month in range(1,13):
                        if month not in airbnb_listing_dict[city_name][listing_id]['per_year'][year]:
                            airbnb_listing_dict[city_name][listing_id]['per_year'][year][month] = np.NaN
                            my_nan_count+=1
                        else:
                            airbnb_listing_dict[city_name][listing_id]['per_year'][year][month] = int(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month]) 
                        print_list_renter.append(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month])
                        monthly_price.append(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month])
                    
                    ### Renters summary - compute averages, min and max 
                    monthly_avg = np.NaN
                    monthly_min = np.NaN
                    monthly_max = np.NaN
                    price_variation = np.NaN
                    if my_nan_count < 12:
                        monthly_price_np = np.asarray(monthly_price)
                        monthly_avg = np.nanmean(monthly_price_np)
                        monthly_min = np.nanmin(monthly_price_np)
                        monthly_max = np.nanmax(monthly_price_np)
                        price_variation = monthly_max - monthly_min
                        
                    print_list_renter.append(monthly_avg)
                    print_list_renter.append(monthly_min)
                    print_list_renter.append(monthly_max)
                    print_list_renter.append(price_variation)
                    #print(airbnb_listing_dict[city][listing_id]['per_year'][year][month],",")

                    if listing_id not in airbnb_investor[city_name][year]['listing_avg_list']:
                        airbnb_investor[city_name][year]['listing_list'].append(listing_id)
                    airbnb_investor[city_name][year]['listing_avg_list'].append(monthly_avg)

                    
                    if year == last_year:
                        csv_out.writerow(print_list_renter)
                        out.flush()
            
            
            #Investors summary 
            print_list_investor = []
            print_list_investor.append(city_name)
            year2 = start_year
            while ( year2 <= end_year):
                listing_count = 0
                city_avg = 0
                if year2 in airbnb_investor[city_name]:
                    listing_count = len(airbnb_investor[city_name][year2]['listing_list'])
                    city_tmp = np.asarray(airbnb_investor[city_name][year2]['listing_avg_list'])
                    city_avg = np.nanmean(city_tmp)
                print_list_investor.append(listing_count)
                print_list_investor.append(city_avg)
                year2 += 1
            csv_out_inv.writerow(print_list_investor)
            inv_out.flush()

    out.close()
    inv_out.close()

    ### uploading to S3 
    s3 = boto3.client('s3')
    bucket_name = 'bnbcleanedv4test'
    s3.upload_file(renter_out,bucket_name,renter_out)
    s3.upload_file(investor_out,bucket_name,investor_out)
    print('File Uploaded to S3')
    os.remove(renter_out)
    os.remove(investor_out)

    ### Remove temp file 
    all_files = glob.glob("mydataallcsv/*")
    for file_name in all_files:
        os.remove(os.path.join(file_name))
    shutil.rmtree("mydataallcsv/")
    return(row_len)


def main():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('bnbcleanedv2')
    s3_path = 's3://bnbcleanedv2/'
        
    city_to_skip = "QWERTY"  ### Used only for debug purpose
    year_to_skip = 2015 
    start_flag = 0
    city_list = []
    row_len = 0

    for my_bucket_object in my_bucket.objects.all():
        current_file = my_bucket_object.key
        current_list = current_file.split('__')
        current_city = current_list[0]
        current_year = int(current_list[1])
        current_month = int(current_list[2])
        ### List of valid cities with listing data between 2017 and 2019   
        if current_city not in city_list:
            if current_year >= 2017 and current_year <= 2019:
                city_list.append(current_city)
    

    for my_city in city_list:
        row_len = bnb_processor(my_city,row_len)

    print(row_len)


if __name__ == "__main__":
    main()
