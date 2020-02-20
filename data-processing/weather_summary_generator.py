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
import string
import re


### To clean city name - remove any special characters, remove known suffixes etc. 
def __clean_cityname__(name):
    name = name.lower()
    name = re.sub(r',.*','',name)
    pat0 = re.compile(r'^ \'')
    name = re.sub(pat0,'',name)
    pat1 = re.compile(r'\s+')
    name = re.sub(pat1,'',name)
    pat2 = re.compile(re.escape('airport'),re.IGNORECASE)
    name = re.sub(pat2,'',name)
    pat2 = re.compile(re.escape('international'),re.IGNORECASE)
    name = re.sub(pat2,'',name)
    pat2 = re.compile(re.escape('downtown'),re.IGNORECASE)
    name = re.sub(pat2,'',name)
    name1 = re.sub(r'\d+.*','',name)
    name1 = re.sub(r'\-','',name1)
    return(name1)


def main():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('testsummary')
    w_path = 's3://testsummarydata/weather_2019_summary.csv'
    s3_bnb = s3.Bucket('bnbcleanedv3')
    
    ### DICT - used for holding the cleaned city-name and actual airbnb or weather data-set names 
    ### at this stage populating only the airbnb city-names with weather city-names = "NA"
    city_map  = {}
    for my_bucket_object in s3_bnb.objects.all():
        current_file = my_bucket_object.key
        current_list = current_file.split('__')
        current_city = current_list[0]
        clean_name = __clean_cityname__(current_city)
        if clean_name not in city_map:
            city_map[clean_name] = {}
            city_map[clean_name]['bnb'] = {}
            city_map[clean_name]['weather'] = {}
            city_map[clean_name]['bnb'] = current_city
            city_map[clean_name]['weather'] = "NA"
            #uniq_cities.append(current_city)


    ### Read weather data-set         
    df = pd.read_csv(w_path,quotechar="'",delimiter=',',usecols =["YEAR","MONTH","CITY","TEMP","PRECIPITATION"])
    weather_out = "weather_summary_filtered.csv"
    with open(weather_out,'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['weather_city','city_name','Year','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']);


        w_dict = {}
        for index,row in df.iterrows():
            year = row[0]
            month = str(row[1])
            month = re.sub(r'\'','',month)
            month = int(month)
            city = row[2]
            temp_avg = str(row[4]) 
            temp_avg = re.sub(r'\s+','',temp_avg)
            ### Skip data-frame if average-temperature is nan or empty  
            if temp_avg == "nan":
                continue
            if temp_avg == '':
                continue
            ### Clean city-name from weather data-set
            clean_name = __clean_cityname__(row[2])
            if clean_name not in w_dict:
                w_dict[clean_name] = {}
                w_dict[clean_name]['city'] = {}
                w_dict[clean_name]['city'] = city
                w_dict[clean_name]['year'] = {}
            if year not in w_dict[clean_name]['year']:
                w_dict[clean_name]['year'][year] = {}
            if month not in w_dict[clean_name]['year'][year]:
                w_dict[clean_name]['year'][year][month] = {}
                w_dict[clean_name]['year'][year][month] = temp_avg
        
            ### DICT - used for holding the cleaned city-name and actual airbnb or weather data-set names
            ### Check if cleaned city-name exists if NO it is missing from the airbnb data 
            if clean_name not in city_map:
                city_map[clean_name] = {}
                city_map[clean_name]['bnb'] = {}
                city_map[clean_name]['weather'] = {}
                city_map[clean_name]['bnb'] = "NA"
                city_map[clean_name]['weather'] = row[2]
            else:
                city_map[clean_name]['weather'] = {}
                city_map[clean_name]['weather'] = row[2]
           
        
        ### Summarize weather data over the year - only for cities in both airbnb and weather data
        for city_name in sorted(w_dict):
            if city_map[city_name]['bnb'] == "NA":
                continue
            print(city_name)
            for year in sorted(w_dict[city_name]['year']):
                print_list = []
                print_list.append(city_name)
                print_list.append(city_map[city_name]['bnb'])
                print_list.append(year)
                for month in range(1,13):
                    if month not in w_dict[city_name]['year'][year]:
                        w_dict[city_name]['year'][year][month] = np.NaN
                    if w_dict[city_name]['year'][year] == " " or w_dict[city_name]['year'][year] == "":
                        w_dict[city_name]['year'][year][month] = np.NaN
                    print_list.append(w_dict[city_name]['year'][year][month])
                csv_out.writerow(print_list)


if __name__ == "__main__":
    main()
