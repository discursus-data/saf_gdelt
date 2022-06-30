import boto3
from urllib.request import urlopen, urlretrieve
import zipfile
import csv

def get_latest_events(s3_bucket_name):
    #Get url for latest events
    print("Get meta info from GDELT")

    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_events_url = latest_updates_text.split('\\n')[0].split(' ')[2]
    latest_events_filename_zip = latest_events_url.split('gdeltv2/')[1]
    latest_events_filename_csv = latest_events_filename_zip.split('.zip')[0]
    latest_events_filedate = latest_events_filename_csv[0:8]
    print(latest_events_filedate)


    #Download and extract latest events
    print("Downloading and extracting latest events")

    urlretrieve(latest_events_url, latest_events_filename_zip)
    with zipfile.ZipFile(latest_events_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')


    # #Save gdelt data to S3
    print("Copying to S3")
    s3 = boto3.resource('s3')
    s3_object_location = 'sources/gdelt/' + latest_events_filedate + '/' + latest_events_filename_csv
    s3.Object(s3_bucket_name, s3_object_location).put(Body = open(latest_events_filename_csv, 'rb'))
    print("Saved latest events to S3: " + s3_object_location)

    return s3_object_location