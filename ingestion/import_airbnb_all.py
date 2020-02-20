import requests
import urllib
import time
import os
import csv
from csv import writer
from csv import reader
import gzip
#import urllib2
from contextlib import closing
import io
from bs4 import BeautifulSoup


def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def file_size(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convert_bytes(file_info.st_size)



url = 'http://insideairbnb.com/get-the-data.html'
response = requests.get(url)
print("URL access request response:",response)
soup = BeautifulSoup(response.text,"html.parser")

sleep_timer = 0
count = 0
count_limit = 10 ### Using this for only debug purpose
start_download_flag = 1 ### Using this for only debug purpose  

### Select only url links to listings.csv.gz 
for link1 in soup.select('a[href$="listings.csv.gz"]'):
        link = link1['href']
        print(link)
        response = requests.get(link)
        ### If error accessing webpage - skip the file link 
        if response.status_code == 404:
                print("-ERROR 404 -:",link)
                continue
        ### Extract City, Year, Month info - from url path 
        data_dir = os.path.dirname(link)
        date_path = os.path.dirname(data_dir)
        date_stamp = os.path.basename(date_path)
        city_path = os.path.dirname(date_path)
        city_name = os.path.basename(city_path)
        yyyy,mm,dd = date_stamp.split("-") 

        if (count < count_limit):  ### Using this only for debug purposes
                y_str = str(yyyy)
                m_str = str(mm)
                filename = city_name + "__" + y_str + "__" + mm + "__listings.csv"
                if (start_download_flag == 1):
                        print(link),
                        print(filename)
                        # Read and append file with year,month
                        line_count = 0
                        web_response = requests.get(link, stream=True)
                        csv_gz_file = web_response.content
                        f = io.BytesIO(csv_gz_file)
                        
                        with gzip.GzipFile(fileobj=f) as fi, \
                                open(filename,'w') as fo:
                                        reader = csv.reader(io.TextIOWrapper(fi,'utf8'), dialect=csv.excel_tab,skipinitialspace=True, delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"',lineterminator="")
                                        csv_writer = writer(fo)
                                        ### Adding columns - Year, Month, makes it easier for next stages
                                        for row in reader:
                                                if line_count == 0:
                                                        row.append("Year")
                                                        row.append("Month")
                                                        line_count+=1
                                                else:
                                                        row.append(y_str)
                                                        row.append(m_str)
                                                csv_writer.writerow(row)
        ### To avoid hitting - limit rate 
        if (sleep_timer < 10):
                sleep_timer+=1
        else:
                sleep_timer=0
                time.sleep(1)

