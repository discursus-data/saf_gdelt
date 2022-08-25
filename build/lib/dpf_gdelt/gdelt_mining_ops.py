from dagster import op

from urllib.request import urlopen, urlretrieve
import zipfile
import pandas as pd


################
# Op to fetch the latest url of GDELT asset
@op
def get_url_to_latest_asset(context):
    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())

    if context.op_config["gdelt_asset"] == "events": 
        latest_asset_url = latest_updates_text.split('\\n')[0].split(' ')[2]
    elif context.op_config["gdelt_asset"] == "mentions":
        latest_asset_url = latest_updates_text.split('\\n')[1].split(' ')[2]

    context.log.info("Mining asset from : " + latest_asset_url)

    return latest_asset_url


################
# Op to build a file path for saving of data assets
@op
def build_file_path(context, gdelt_asset_url):
    gdelt_asset_filename_zip = str(gdelt_asset_url).split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]
    gdelt_asset_filedate = gdelt_asset_filename_csv[0:8]
    gdelt_asset_file_path = 'sources/gdelt/' + gdelt_asset_filedate + '/' + gdelt_asset_filename_csv

    context.log.info("Will save data asset to this path : " + gdelt_asset_file_path)

    return gdelt_asset_file_path


################
# Op to mine the latest asset from GDELT
@op
def mine_latest_asset(context, gdelt_asset_url):
    context.log.info("Downloading and extracting latest asset")
    
    gdelt_asset_filename_zip = gdelt_asset_url.split('gdeltv2/')[1]
    gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]

    urlretrieve(gdelt_asset_url, gdelt_asset_filename_zip)
    with zipfile.ZipFile(gdelt_asset_filename_zip, 'r') as zip_ref:
        zip_ref.extractall('.')
    df_latest_asset  = pd.read_csv(gdelt_asset_filename_csv, sep = '\t', header = None)

    context.log.info("Mined : " + str(len(df_latest_asset)) + " rows from asset")

    return df_latest_asset



################
# Op to filter the latest events from GDELT using the passed configs
@op
def filter_latest_events(context, df_latest_events):
    context.log.info("Filtering latest events")
    
    filter_condition_event_code = context.op_config["filter_event_code"]
    filter_condition_countries = list(context.op_config["filter_countries"])
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


################
# Op to filter the latest mentions from GDELT using the filtered list of events
@op
def filter_latest_mentions(context, df_latest_mentions, df_latest_events_filtered):
    context.log.info("Filtering latest mentions")
    
    df_latest_mentions_filtered = df_latest_mentions
    df_latest_mentions_filtered = df_latest_mentions_filtered[(df_latest_mentions_filtered.iloc[:,0].isin(df_latest_events_filtered.iloc[:,0]))]
    context.log.info("We now have " + str(len(df_latest_mentions_filtered)) + " remaining mentions out of " + str(len(df_latest_mentions)))

    return df_latest_mentions_filtered


################
# Op to filter the latest gkg from GDELT using the filtered list of events
@op
def filter_latest_gkg(context, df_latest_gkg, df_latest_events_filtered):
    context.log.info("Filtering latest gkg")
    
    df_latest_gkg_filtered = df_latest_gkg
    df_latest_gkg_filtered = df_latest_gkg_filtered[(df_latest_gkg_filtered.iloc[:,1].isin(df_latest_events_filtered.iloc[:,0]))]
    context.log.info("We now have " + str(len(df_latest_gkg_filtered)) + " remaining gkg out of " + str(len(df_latest_gkg)))

    return df_latest_gkg_filtered