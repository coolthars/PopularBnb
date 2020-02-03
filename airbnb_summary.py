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
from pyspark import SparkConf                                                                                                                 
from pyspark.context import SparkContext                                                                                                      
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.appName("Py").getOrCreate()

### Read all CSVs for a given year into Spark DataFrames - get per Calendar Month price for each Listing


def main():
    print("Running AirBNB Data Processor")
    summary_year = 2019
    #path = "s3a://bnbpipecleaned/amsterdam*2019*"
    #path = "s3a://bnbpipecleaned/*"
    path = "s3a://bnbcleanedv2/amsterdam*2019*"
    cleaned_df = spark.read.format('csv').options(header='true', inferSchema='true').load(path)
    #f.write.csv('2019all.csv')
    #cleaned_df.coalesce(1).write.mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").csv("Summary_BNB_2019.csv")
    cleaned_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydataallcsv")

    #for index, row in cleaned_df.iterrows():
    #    print(row['city'],row['listing_id'],row['Year'], row['Month'], row['id'], row['price'], row['number_of_reviews'],row['reviews_per_month'],row['listing_url'])

    all_files = glob.glob("mydataallcsv/*csv")
    #for ff in all_files:
    #    print("File-----:",ff)


    airbnb_listing_dict = {}
    airbnb_investor = {}

    info_list = ['country','state','city','listing_url','host_id','host_url','calculated_host_listings_count','number_of_reviews']
    
    df = []
    for file_name in all_files:
        print("Info: Parsing File",file_name)
        df_tmp = pd.read_csv(file_name,usecols =["id","listing_url","host_id","host_url","host_name","city_name","city","state","country","price","number_of_reviews","review_scores_rating","calculated_host_listings_count","reviews_per_month","Year","Month"])
        df_tmp = df_tmp[np.isfinite(df_tmp['number_of_reviews'])]
        df_tmp = df_tmp[np.isfinite(df_tmp['Month'])]
        #df_tmp['price'] = df['price'].str.replace('$','')
        #df_tmp['price'] = df['price'].astype(float)
        #df1['Avg_Annual'] = df1['Avg_Annual'].str.replace('$', '')
        #df_tmp = df_tmp[np.isfinite(df_tmp['month'])]
        df_tmp['price'] = df_tmp['price'].str.replace(',', '').str.replace('$', '').astype(float)
        df.append(df_tmp)
        
    concat_df = pd.concat(df, axis = 0, ignore_index=True)

    counti = 0
    #start_year = concat_df['Year'].min()
    #end_year = concat_df['Year'].max()
    start_year = 2018
    end_year = 2019

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
                #if info_param == "number_of_reviews":
                #    if 
                #airbnb_listing_dict[row['city']][row['id']]['info'][''] = row['listing_url']
                ### Need to add all other stuff like host etc.. under this level i.e. info --> DONE
        if row['Year'] not in airbnb_listing_dict[row['city_name']][row['id']]['per_year']:
            airbnb_listing_dict[row['city_name']][row['id']]['per_year'][row['Year']] = {}
        #if row['Month'] not in airbnb_listing_dict[row['city']][row['id']]['per_year'][row['Year']]:
            #airbnb_listing_dict[row['city']][row['id']]['per_year'][row['Year']][int(row['Month'])] = {}
        #print(row['city_name'],row['id'],row['Year'],int(row['Month'])
        try:
            airbnb_listing_dict[row['city_name']][row['id']]['per_year'][row['Year']][int(row['Month'])] = row['price']
            #break
        except:
            print(counti,row['city_name'],row['id'],row['Year'],row['Month'])        

            #print("Adding month = :",row['Month'])
        #else:
            #print("Y am i skipping this = :",row['Month'])

    now = datetime.datetime.now()
    print(now.year)
    last_year = now.year - 1
    print(last_year)



    ### Find a way to print this to CSV
    with open("renter_summary.csv",'w') as out, open("investor_summary.csv",'w') as inv_out:
        csv_out = csv.writer(out)
        csv_out_inv = csv.writer(inv_out)
        csv_out.writerow(['city_name','listing_id','country','state','city','listing_url','host_id','host_url','calculated_host_listings_count','number_of_reviews','Year','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','AVG','MIN','MAX','Price_Variation']);
        #print("city,listing_id,country,state,listing_url,host_id,host_url,host_since,number_of_reviews,Year,price_01,price_02,price_03,price_04,price_05,price_06,price_07,price_08,price_09,price_10,price_11,price_12")
        #month_list = ["01","02","03","04","05","06","07","08","09","10","11","12"]
        #month_list =["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        
        year1 = start_year
        print_inv_header = []
        print_inv_header.append("city_name")
        year1 = start_year
        while ( year1 <= end_year):
            l_count = str(year1) + "__listings_count"
            l_avg = str(year1) + "__average_price"
            print_inv_header.append(l_count)
            print_inv_header.append(l_avg)
        csv_out_inv.writerow(print_inv_header)


        for city_name in sorted(airbnb_listing_dict):
            for listing_id in sorted(airbnb_listing_dict[city_name]):
                for year in airbnb_listing_dict[city_name][listing_id]['per_year']:

                    if year not in airbnb_investor[city_name]:
                        airbnb_investor[city_name][year] = {}
                        airbnb_investor[city_name][year]['listing_list'] = []
                        airbnb_investor[city_name][year]['listing_avg_list'] = []
                        #airbnb_investor[city_name']][row['Year']]['listing_count'] = 0
                
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
                            #airbnb_listing_dict[city_name][listing_id]['per_year'][year][month] = airbnb_listing_dict[city_name][listing_id]['per_year'][year][month].strip().lstrip(",")
                            #airbnb_listing_dict[city_name][listing_id]['per_year'][year][month] = float(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month].strip().lstrip("$")) 
                        print_list_renter.append(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month])
                        monthly_price.append(airbnb_listing_dict[city_name][listing_id]['per_year'][year][month])
                        
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
                    #csv_out_inv.writerow(print_list_renter)
            
            
            #break
            print_list_investor = []
            print_list_investor.append(city_name)
            year = start_year
            while ( year <= end_year):
                listing_count = 0
                city_avg = 0
                if year in airbnb_investor[city_name][year]:
                    listing_count = len(airbnb_investor[city_name][year]['listing_list'])
                    city_tmp = np.asarray(airbnb_investor[city_name][year]['listing_avg_list'])
                    city_avg = np.nanmean(airbnb_investor[city_name][year]['listing_avg_list'])
                print_list_investor.append(listing_count)
                print_list_investor.append(city_avg)
            csv_out_inv.writerow(print_list_investor)


    out.close()
    



if __name__ == "__main__":
    main()
