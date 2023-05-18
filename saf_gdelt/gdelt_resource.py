from dagster import resource

from urllib.request import urlopen, urlretrieve
import zipfile
import pandas as pd



class GDELTResource:
    # GDELT methods
    ######################
    def get_url_to_latest_asset(self, gdelt_asset):
        # Fetches the latest url of GDELT asset

        latest_updates_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        latest_updates_text = str(urlopen(latest_updates_url).read())

        latest_asset_url = None  # Assign a default value

        if gdelt_asset == "events": 
            latest_asset_url = latest_updates_text.split('\\n')[0].split(' ')[2]
        elif gdelt_asset == "mentions":
            latest_asset_url = latest_updates_text.split('\\n')[1].split(' ')[2]
        elif gdelt_asset == "gkg":
            latest_asset_url = latest_updates_text.split('\\n')[2].split(' ')[2]

        return latest_asset_url
    

    
    def mine_latest_asset(self, gdelt_asset_url):
        # Mines the latest asset from GDELT
        
        gdelt_asset_filename_zip = gdelt_asset_url.split('gdeltv2/')[1]
        gdelt_asset_filename_csv = gdelt_asset_filename_zip.split('.zip')[0]

        urlretrieve(gdelt_asset_url, gdelt_asset_filename_zip)
        with zipfile.ZipFile(gdelt_asset_filename_zip, 'r') as zip_ref:
            zip_ref.extractall('.')
        df_latest_asset  = pd.read_csv(gdelt_asset_filename_csv, sep = '\t', header = None)

        return df_latest_asset



    def filter_latest_events(self, df_latest_events, filter_event_code, filter_countries):
        # Filters the latest events from GDELT using the passed configs
        
        filter_condition_event_code = filter_event_code
        filter_condition_countries = list(filter_countries)
        df_latest_events_filtered = df_latest_events

        if filter_condition_event_code:
            df_latest_events_filtered = df_latest_events_filtered[(df_latest_events_filtered.iloc[:,28] == filter_condition_event_code)]
        if filter_condition_countries:
            df_latest_events_filtered = df_latest_events_filtered[(df_latest_events_filtered.iloc[:,53].isin(filter_condition_countries))]

        return df_latest_events_filtered


    def filter_latest_mentions(self, df_latest_mentions, df_latest_events_filtered):
        # Filter the latest mentions from GDELT using the filtered list of events
        
        df_latest_mentions_filtered = df_latest_mentions
        df_latest_mentions_filtered = df_latest_mentions_filtered[(df_latest_mentions_filtered.iloc[:,0].isin(df_latest_events_filtered.iloc[:,0]))]

        return df_latest_mentions_filtered


    def filter_latest_gkg(self, df_latest_gkg, df_latest_events_filtered):
        # Filters the latest gkg from GDELT using the filtered list of events

        df_latest_gkg_filtered = df_latest_gkg
        df_latest_gkg_filtered = df_latest_gkg_filtered[(df_latest_gkg_filtered.iloc[:,1].isin(df_latest_events_filtered.iloc[:,0]))]

        return df_latest_gkg_filtered


@resource(
    description="A GDELT resource.",
)
def initiate_gdelt_resource(context):
    return GDELTResource()