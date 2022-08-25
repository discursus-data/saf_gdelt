# The Lantrns Analytics' data product framework - GDELT library
This library provides [ops](https://docs.dagster.io/concepts/ops-jobs-graphs/ops) to source the [GDELT](https://www.gdeltproject.org/) public data source.

It is part of the [Lantrns Analytics' data product framework](https://github.com/lantrns-analytics/dpf_core). Please visit the repo for more information. And visit us at [lantrns.co](https://www.lantrns.co) for more context on our mission.

&nbsp;

# How to use this library
Please refer to the [Lantrns Analytics' data product framework](https://github.com/lantrns-analytics/dpf_core) instructions for how to use a the framework and its libraries.

&nbsp;

# Library Ops
The library includes the following ops.

## gdelt_mining_ops.get_url_to_latest_asset
Fetches the latest url for a specific GDELT asset.

Parameters
- None required.

Configurations
- gdelt_asset: Which GDELT asset to mine. Values can either be `events`, `mentions` or `gkg`

## gdelt_mining_ops.build_file_path
Builds a file path for saving of data assets.

Parameters
- gdelt_asset_url: Output from `get_url_to_latest_asset`

Configurations
- None required.

## gdelt_mining_ops.mine_latest_asset
Mines the latest asset from GDELT.

Parameters
- gdelt_asset_url: Output from `get_url_to_latest_asset`

Configurations
- None required.

## gdelt_mining_ops.filter_latest_events
Filters the latest events from GDELT using the passed configs.

Parameters
- df_latest_events: Output from `mine_latest_asset`

Configurations
- filter_event_code: Event code to mine (e.g. 14 for protest events)
- filter_countries: Country codes to mine (e.g. US)

## gdelt_mining_ops.filter_latest_mentions
Filters the latest mentions from GDELT using the filtered list of events.

Parameters
- df_latest_mentions: Output from `mine_latest_asset`
- df_latest_events_filtered: Output from `df_latest_events_filtered`

Configurations
- None required.

## gdelt_mining_ops.filter_latest_gkg
Filters the latest gkg from GDELT using the filtered list of events.

Parameters
- df_latest_gkg: Output from `mine_latest_asset`
- df_latest_events_filtered: Output from `df_latest_events_filtered`

Configurations
- None required.

&nbsp;

# Development of library
- Once improvements have been added to library
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch
