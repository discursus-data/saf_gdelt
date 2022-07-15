from dagster import op, AssetMaterialization, Output
from dagster_shell import create_shell_command_op
from dagster import file_relative_path

import boto3
from urllib.request import urlopen, urlretrieve
import zipfile
from io import StringIO
import pandas as pd

from discursus_gdelt import content_auditor

######################
# MINING OPS
######################
# Op to fetch the latest url of GDELT data files
@op
def get_latest_events_url(context):
    context.log.info("Get meta info from GDELT")

    latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    latest_updates_text = str(urlopen(latest_updates_url).read())
    latest_events_url = latest_updates_text.split('\\n')[0].split(' ')[2]

    context.log.info("Mining from : " + latest_events_url)

    return latest_events_url


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
def mine_latest_mentions(context, latest_events_url):
    context.log.info("Downloading and extracting latest mentions")
    
    latest_mentions_filename_zip = latest_events_url.split('gdeltv2/')[1]
    context.log.info("Latest mentions zip filename is : " + latest_mentions_filename_zip)
    latest_mentions_filename_csv = latest_mentions_filename_zip.split('.zip')[0]
    context.log.info("Latest mentions csv filename is : " + latest_mentions_filename_csv)

    urlretrieve(latest_events_url, latest_mentions_filename_zip)
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


# Op to get the meta data from a list of urls
@op(
    required_resource_keys = {
        "aws_client",
        "gdelt_client"
    }
)
def enhance_articles(context, latest_gdelt_events_s3_location):
    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()
    event_code = context.resources.gdelt_client.get_event_code()
    countries = context.resources.gdelt_client.get_countries()

    # Extracting which file we're enhancing
    filename = latest_gdelt_events_s3_location.splitlines()[-1]

    # Get a unique list of urls to enhance
    context.log.info("Targeting the following events: " + str(event_code))
    context.log.info("Targeting the following countries: " + str(countries))
    content_bot = content_auditor.ContentAuditor(s3_bucket_name, filename)
    content_bot.get_list_of_urls(event_code, countries)

    # Enhance urls
    context.log.info("Enhancing " + str(len(content_bot.article_urls)) + " articles")
    content_bot.read_url()

    # Create dataframe
    df_gdelt_enhanced_articles = pd.DataFrame (content_bot.site_info, columns = ['mention_identifier', 'page_name', 'file_name', 'page_title', 'page_description', 'keywords'])
    context.log.info("Enhanced " + str(df_gdelt_enhanced_articles['mention_identifier'].size) + " articles")

    # Save enhanced urls to S3
    context.log.info("Exporting to S3")
    content_bot.write_to_spreadsheet(s3_bucket_name, filename)

    return df_gdelt_enhanced_articles




######################
# SAVINGS OPS
######################
# Op to save the latest GDELT events to S3
@op(
    required_resource_keys = {
        "aws_client"
    }
)
def save_gdelt_events(context, df_latest_events, latest_events_url):
    context.log.info("Saving latest events to S3")

    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()

    latest_events_filename_zip = latest_events_url.split('gdeltv2/')[1]
    latest_events_filename_csv = latest_events_filename_zip.split('.zip')[0]
    latest_events_filedate = latest_events_filename_csv[0:8]
    
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    df_latest_events.to_csv(csv_buffer, index = False)
    latest_events_s3_object_location = 'sources/gdelt/' + latest_events_filedate + '/' + latest_events_filename_csv
    s3.Object(s3_bucket_name, latest_events_s3_object_location).put(Body=csv_buffer.getvalue())


    context.log.info("Saved latest events to : " + latest_events_s3_object_location)

    return latest_events_s3_object_location


# Op to materialize the latest GDELT events as a data asset in Dagster
@op(
    required_resource_keys = {
        "aws_client"
    }
)
def materialize_gdelt_mining_asset(context, latest_events_s3_object_location, df_latest_events_filtered):
    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()

    # Extracting which file we're materializing
    filename = latest_events_s3_object_location.splitlines()[-1]
    
    # Materialize asset
    yield AssetMaterialization(
        asset_key = ["sources", "gdelt_events"],
        description = "List of events mined on GDELT",
        metadata={
            "path": "s3://" + s3_bucket_name + "/" + filename,
            "rows": df_latest_events_filtered.index.size
        }
    )
    yield Output(df_latest_events_filtered)


# Op to materialize the url metadata as a data asset in Dagster
@op(
    required_resource_keys = {
        "aws_client"
    }
)
def materialize_enhanced_articles_asset(context, df_gdelt_enhanced_articles, latest_gdelt_events_s3_location):
    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()

    # Extracting which file we're enhancing
    filename = latest_gdelt_events_s3_location.splitlines()[-1]

    # Materialize asset
    yield AssetMaterialization(
        asset_key=["sources", "gdelt_articles"],
        description="List of enhanced articles mined from GDELT",
        metadata={
            "path": "s3://" + s3_bucket_name + "/" + filename.split(".")[0] + "." + filename.split(".")[1] + ".enhanced.csv",
            "rows": df_gdelt_enhanced_articles['mention_identifier'].size
        }
    )
    yield Output(df_gdelt_enhanced_articles)
