import boto3
from urllib.request import urlopen, urlretrieve
import zipfile
import csv
from io import StringIO
import pandas as pd

def get_latest_events_url():
    #Get url for latest events
    print("Get meta info from GDELT")

    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_events_url = latest_updates_text.split('\\n')[0].split(' ')[2]

    return latest_events_url


def mine_latest_events(latest_events_url):
    latest_events_filename_zip = latest_events_url.split('gdeltv2/')[1]
    latest_events_filename_csv = latest_events_filename_zip.split('.zip')[0]
    latest_events_filedate = latest_events_filename_csv[0:8]

    #Download and extract latest events
    print("Downloading and extracting latest events")

    urlretrieve(latest_events_url, latest_events_filename_zip)
    with zipfile.ZipFile(latest_events_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')

    #Read csv into dataframe
    df_latest_events  = pd.read_csv(latest_events_filename_csv, sep='\t')

    return df_latest_events


def save_latest_events(s3_bucket_name, df_latest_events, latest_events_url):
    latest_events_filename_zip = latest_events_url.split('gdeltv2/')[1]
    latest_events_filename_csv = latest_events_filename_zip.split('.zip')[0]
    latest_events_filedate = latest_events_filename_csv[0:8]
    
    #Save gdelt data to S3
    print("Copying to S3")
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    df_latest_events.to_csv(csv_buffer, index = False)
    latest_events_s3_object_location = 'sources/gdelt/' + latest_events_filedate + '/' + latest_events_filename_csv
    s3.Object(s3_bucket_name, latest_events_s3_object_location).put(Body=csv_buffer.getvalue())
    print("Saved latest events to S3: " + latest_events_s3_object_location)

    return latest_events_s3_object_location


def get_latest_mentions(s3_bucket_name):
    #Get url for latest mentions
    print("Get meta info from GDELT")

    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_mentions_url = latest_updates_text.split('\\n')[1].split(' ')[2]
    latest_mentions_filename_zip = latest_mentions_url.split('gdeltv2/')[1]
    latest_mentions_filename_csv = latest_mentions_filename_zip.split('.zip')[0]
    latest_mentions_filedate = latest_mentions_filename_csv[0:8]
    print(latest_mentions_filedate)


    #Download and extract latest mentions
    print("Downloading and extracting latest mentions")

    urlretrieve(latest_mentions_url, latest_mentions_filename_zip)
    with zipfile.ZipFile(latest_mentions_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')


    #Save gdelt data to S3
    print("Copying to S3")
    s3 = boto3.resource('s3')
    s3_object_location = 'sources/gdelt/' + latest_mentions_filedate + '/' + latest_mentions_filename_csv
    s3.Object(s3_bucket_name, s3_object_location).put(Body = open(latest_mentions_filename_csv, 'rb'))
    print("Saved latest mentions to S3: " + s3_object_location)

    return s3_object_location