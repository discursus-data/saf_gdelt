# The OSS Social Analytics Framework - GDELT library
This library provides a client [resource](https://docs.dagster.io/concepts/resources) to interact with the [GDELT](https://www.gdeltproject.org/) public data source.

It is part of the [Social Analytics Framework](https://github.com/lantrns-analytics/saf_core). ease visit the repo for more information on the framework, its mission and how to use it.

&nbsp;


# Library

# Methods
## gdelt_resource.initiate_gdelt_client
Initialize client to interact with the GDELT public data source

Configurations:
- None

Example:
```
my_gdelt_client = gdelt_resource.initiate_gdelt_client()
```

## gdelt_resource.get_url_to_latest_asset
Fetches the latest url for a specific GDELT asset.

Parameters:
- gdelt_asset: Which GDELT asset to mine. Values can either be `events`, `mentions` or `gkg`

Returns:
- latest_asset_url: URL of latest asset

Example:
```
latest_asset_url = context.resources.gdelt_client.get_url_to_latest_asset(gdelt_asset)
```

## gdelt_resource.build_file_path
Builds a file path for saving of data assets.

Parameters:
- gdelt_asset_url: URL of latest asset

Returns:
- gdelt_asset_file_path: File path where data asset is saved

Example:
```
gdelt_asset_file_path = context.resources.gdelt_client.build_file_path(gdelt_asset_url)
```

## gdelt_resource.mine_latest_asset
Mines the latest asset from GDELT.

Parameters:
- gdelt_asset_url: URL of latest asset

Returns:
- df_latest_asset: Dataframe of latest asset

Example:
```
df_latest_asset = context.resources.gdelt_client.mine_latest_asset(gdelt_asset_url)
```

## gdelt_resource.filter_latest_events
Filters the latest events from GDELT using the passed configs.

Parameters:
- df_latest_events: Dataframe of latest events asset
- filter_event_code: Event code of events to keep
- filter_countries: Country codes of events to keep

Returns:
- df_latest_events_filtered: Filtered dataframe of latest events asset

Example:
```
df_latest_events_filtered = context.resources.gdelt_client.filter_latest_events(df_latest_events, filter_event_code, filter_countries)
```

## gdelt_resource.filter_latest_mentions
Filters the latest mentions from GDELT using the filtered list of events.

Parameters:
- df_latest_mentions: Dataframe of latest mentions asset
- df_latest_events_filtered: Filtered dataframe of latest events asset

Returns:
- df_latest_mentions_filtered: Filtered dataframe of latest mentions asset

Example:
```
df_latest_mentions_filtered = context.resources.gdelt_client.filter_latest_mentions(df_latest_mentions, df_latest_events_filtered)
```

## gdelt_resource.filter_latest_gkg
Filters the latest gkg from GDELT using the filtered list of events.

Parameters:
- df_latest_gkg: Dataframe of latest gkg asset
- df_latest_events_filtered: Filtered dataframe of latest events asset

Returns:
- df_latest_gkg_filtered: Filtered dataframe of latest gkg asset

Example:
```
df_latest_gkg_filtered = context.resources.gdelt_client.filter_latest_gkg(df_latest_gkg, df_latest_events_filtered)
```

&nbsp;

# Development of library
- Once improvements have been added to library
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch
