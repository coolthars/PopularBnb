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
# Connect to the URL
response = requests.get(url)
print("URL access request response:",response)
soup = BeautifulSoup(response.text,"html.parser")
#soup.findAll('a')
#one_a_tag = soup.findAll('a')[200]
#link = one_a_tag['href']
#print(one_a_tag)
#print(link)

sleep_timer = 0
count = 0
count_limit = 10 ### Using this just for local testing purpose
start_download_flag = 0

for link1 in soup.select('a[href$="listings.csv.gz"]'):
        link = link1['href']
        print(link)
        response = requests.get(link)
        if response.status_code == 404:
                print("-ERROR 404 -:",link)
                continue
        data_dir = os.path.dirname(link)
        date_path = os.path.dirname(data_dir)
        date_stamp = os.path.basename(date_path)
        city_path = os.path.dirname(date_path)
        city_name = os.path.basename(city_path)
        #year_month = extract_month_year(date_stamp)
        yyyy,mm,dd = date_stamp.split("-")
        #time.sleep(1)
        if (count < count_limit):
                #print "My Link:", link
                #count = count + 1
                #print "City: ", city_name
                #print "Month: ", mm
                #print "Year: ", yyyy
                y_str = str(yyyy)
                m_str = str(mm)
                filename = city_name + "__" + y_str + "__" + mm + "__listings.csv"
                if city_name == "madrid":
                        start_download_flag = 1
                if (start_download_flag == 1):
                        print(link),
                        print(filename)
                        # Read and append file with year,month
                        line_count = 0

                        #with open(requests.get(link),'rb') as fi:
                        #       for x in fi.readlines():
                        #               if (line_count == 0):
                        #                       line_count+=1
                        #                       file_lines = [''.join("Year","Month",[x.strip()],"\n")]
                        #               else:
                        #                       file_lines = [''.join(y_str,m_str,[x.strip()],"\n")] 

                        web_response = requests.get(link, stream=True)
                        csv_gz_file = web_response.content
                        f = io.BytesIO(csv_gz_file)
                        #f = io.open(csv_gz_file,"rb")
                        #with open(web_response,'rU') as fi, \
                        #with closing(requests.get(link, stream=True)) as fi, \
                        with gzip.GzipFile(fileobj=f) as fi, \
                                open(filename,'w') as fo:
                                        #reader = csv.reader(fh)
                                        reader = csv.reader(io.TextIOWrapper(fi,'utf8'), dialect=csv.excel_tab,skipinitialspace=True, delimiter=',', quoting=csv.QUOTE_MINIMAL, quotechar='"',lineterminator="")
                                        #reader = csv.reader(fi, dialect=csv.excel_tab)
                                        csv_writer = writer(fo)

                                        for row in reader:
                                                if line_count == 0:
                                                        #print("Year","Month",row)
                                                        row.append("Year")
                                                        row.append("Month")
                                                        line_count+=1
                                                else:
                                                        row.append(y_str)
                                                        row.append(m_str)
                                                        #print(y_str,m_str,row)
                                                #print(row)
                                                csv_writer.writerow(row)




                        #with open(filename, "wb") as f:
                                #f.writelines(requests.get(link))
                                #f.writelines(file_lines)
                                #my_url = urllib2.urlopen(link)
                                #cr = csv.reader(my_url)
                                #cr = csv.reader(requests.get(link))
                                #for row in cr:
                                #       print(y_str,m_str,row)

                        #print("File size in bytes of a plain file: ",file_size(filename))
        if (sleep_timer < 10):
                sleep_timer+=1

        else:
                sleep_timer=0
                time.sleep(1)

