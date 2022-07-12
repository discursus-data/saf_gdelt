from dagster import op, AssetMaterialization, Output
from dagster_shell import create_shell_command_op
from dagster import file_relative_path

import boto3
from io import StringIO
import pandas as pd

from discursus_gdelt import gdelt_miners
from content_auditor import ContentAuditor

@op(
    required_resource_keys = {
        "aws_client",
        "gdelt_client"
    },
    out = {
        "df_latest_events": Out(), 
        "latest_events_filedate": Out(), 
        "latest_events_filename_csv": Out()
    }
)
def mine_gdelt_events(context):
    df_latest_events, latest_events_filedate, latest_events_filename_csv = gdelt_miners.get_latest_events()

    return df_latest_events, latest_events_filedate, latest_events_filename_csv


@op(
    required_resource_keys = {
        "aws_client",
        "gdelt_client"
    },
    out = {
        "df_latest_events": Out(), 
        "s3_object_location": Out()
    }
)
def save_gdelt_events(context, df_latest_events, latest_events_filedate, latest_events_filename_csv):
    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()

    df_latest_events, s3_object_location = gdelt_miners.save_latest_events(s3_bucket_name, df_latest_events, latest_events_filedate, latest_events_filename_csv)

    return df_latest_events, s3_object_location


@op(
    required_resource_keys = {
        "aws_client",
        "gdelt_client"
    }
)
def materialize_gdelt_mining_asset(context, df_latest_events, latest_gdelt_events_s3_location):
    s3_bucket_name = context.resources.aws_client.get_s3_bucket_name()

    # Extracting which file we're materializing
    filename = latest_gdelt_events_s3_location.splitlines()[-1]
    
    # Materialize asset
    yield AssetMaterialization(
        asset_key = ["sources", "gdelt_events"],
        description = "List of events mined on GDELT",
        metadata={
            "path": "s3://" + s3_bucket_name + "/" + filename,
            "rows": df_latest_events.index.size
        }
    )
    yield Output(df_latest_events)


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
    content_bot = ContentAuditor(s3_bucket_name, filename)
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


@op(
    required_resource_keys = {
        "aws_client",
        "gdelt_client"
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
