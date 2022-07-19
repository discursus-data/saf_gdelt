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
Op to fetch the latest url for a specific GDELT asset.

Configurations required:
- gdelt_asset: Which GDELT asset to mine. Values can either be `events`, `mentions` or `gkg`

## gdelt_mining_ops.build_file_path
Op to build a file path for saving of data assets

## gdelt_mining_ops.mine_latest_asset
Op to mine the latest asset from GDELT

## gdelt_mining_ops.filter_latest_events
Op to filter the latest events from GDELT using the passed configs

## gdelt_mining_ops.filter_latest_mentions
Op to filter the latest mentions from GDELT using the filtered list of events

&nbsp;

# Development of library
- Once improvements have been added to library
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch