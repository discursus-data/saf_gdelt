# discursus GDELT library
This library provides [ops](https://docs.dagster.io/concepts/ops-jobs-graphs/ops) to source the [GDELT](https://www.gdeltproject.org/) public data source.

It is part of the [discursus Social Analytics OSS Framework](https://github.com/discursus-io/discursus_core). Please visit the repo for more information. And visit us at [discursus.io] for more context on our mission.

&nbsp;

# How to use this library
Please refer to the [discursus Social Analytics OSS Framework](https://github.com/discursus-io/discursus_core) instructions for how to use a the framework and its libraries.

&nbsp;

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
- None required.

Configurations
- None required.

## gdelt_mining_ops.filter_latest_mentions
Filters the latest mentions from GDELT using the filtered list of events.

Parameters
- None required.

Configurations
- None required.

&nbsp;

# Development of library
- Once improvements have been added to library
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch