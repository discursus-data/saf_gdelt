from dagster import op, AssetMaterialization, Output
from dagster_shell import create_shell_command_op
from dagster import file_relative_path

from urllib.request import urlopen, urlretrieve
import zipfile
from io import StringIO
import pandas as pd


######################
# MINING OPS
######################
# Op to fetch the latest url of GDELT event files
@op
def get_latest_events_url(context):
    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_events_url = latest_updates_text.split('\\n')[0].split(' ')[2]

    context.log.info("Mining events from : " + latest_events_url)

    return latest_events_url


# Op to fetch the latest url of GDELT mentions files
@op
def get_latest_mentions_url(context):
    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_mentions_url = latest_updates_text.split('\\n')[1].split(' ')[2]

    context.log.info("Mining mentions from : " + latest_mentions_url)

    return latest_mentions_url


# Op to mine the latest events from GDELT
@op
def mine_latest_events(context, latest_events_url):
    context.log.info("Downloading and extracting latest events")
    
    latest_events_filename_zip = latest_events_url.split('gdeltv2/')[1]
    context.log.info("Latest events zip filename is : " + latest_events_filename_zip)
    latest_events_filename_csv = latest_events_filename_zip.split('.zip')[0]
    context.log.info("Latest events csv filename is : " + latest_events_filename_csv)

    urlretrieve(latest_events_url, latest_events_filename_zip)
    with zipfile.ZipFile(latest_events_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')
    df_latest_events  = pd.read_csv(latest_events_filename_csv, sep = '\t', header = None)

    context.log.info("Mined : " + str(len(df_latest_events)) + " events")

    return df_latest_events


# Op to mine the latest mentions from GDELT
@op
def mine_latest_mentions(context, latest_mentions_url):
    context.log.info("Downloading and extracting latest mentions")
    
    latest_mentions_filename_zip = latest_mentions_url.split('gdeltv2/')[1]
    context.log.info("Latest mentions zip filename is : " + latest_mentions_filename_zip)
    latest_mentions_filename_csv = latest_mentions_filename_zip.split('.zip')[0]
    context.log.info("Latest mentions csv filename is : " + latest_mentions_filename_csv)

    urlretrieve(latest_mentions_url, latest_mentions_filename_zip)
    with zipfile.ZipFile(latest_mentions_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')
    df_latest_mentions  = pd.read_csv(latest_mentions_filename_csv, sep = '\t', header = None)

    context.log.info("Mined : " + str(len(df_latest_mentions)) + " mentions")

    return df_latest_mentions



######################
# MANIPULATION OPS
######################
# Op to filter the latest events from GDELT using the passed configs
@op(
    required_resource_keys = {
        "gdelt_client"
    }
)
def filter_latest_events(context, df_latest_events):
    context.log.info("Filtering latest events")
    
    filter_condition_event_code = context.resources.gdelt_client.get_event_code()
    filter_condition_countries = list(context.resources.gdelt_client.get_countries())
    df_latest_events_filtered = df_latest_events

    if filter_condition_event_code:
        context.log.info("Filtering latest events by events: " + str(filter_condition_event_code))
        df_latest_events_filtered = df_latest_events_filtered[(df_latest_events_filtered.iloc[:,28] == filter_condition_event_code)]
        context.log.info("We now have " + str(len(df_latest_events_filtered)) + " remaining events out of " + str(len(df_latest_events)))
    if filter_condition_countries:
        context.log.info("Filtering latest events by countries: " + str(filter_condition_countries))
        df_latest_events_filtered = df_latest_events_filtered[(df_latest_events_filtered.iloc[:,53].isin(filter_condition_countries))]
        context.log.info("We now have " + str(len(df_latest_events_filtered)) + " remaining events out of " + str(len(df_latest_events)))

    return df_latest_events_filtered


# Op to filter the latest mentions from GDELT using the filtered list of events
@op(
    required_resource_keys = {
        "gdelt_client"
    }
)
def filter_latest_mentions(context, df_latest_mentions, df_latest_events_filtered):
    context.log.info("Filtering latest mentions")
    
    df_latest_mentions_filtered = df_latest_mentions
    df_latest_mentions_filtered = df_latest_mentions_filtered[(df_latest_mentions_filtered.iloc[:,0].isin(df_latest_events_filtered.iloc[:,0]))]
    context.log.info("We now have " + str(len(df_latest_mentions_filtered)) + " remaining mentions out of " + str(len(df_latest_mentions)))

    return df_latest_mentions_filtered