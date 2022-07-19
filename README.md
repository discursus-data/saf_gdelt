# discursus GDELT library
This library provides [ops](https://docs.dagster.io/concepts/ops-jobs-graphs/ops) to source the [GDELT](https://www.gdeltproject.org/) public data source.

It is part of the [discursus Social Analytics OSS Framework](https://github.com/discursus-io/discursus_core). Please visit the repo for more information. And visit us at [discursus.io] for more context on our mission.

&nbsp;

# How to use this library
We assume you are running a Docker file such as the one we have in the [Core repo](https://github.com/discursus-io/discursus_core/blob/release/0.1/Dockerfile_app.REPLACE).

The only thing you need to add is this line that will load the GDELT library to your instance of the social analytics framework.

`RUN pip3 install git+https://github.com/discursus-io/discursus_gdelt@release/0.1`

Please refer to the [discursus Social Analytics OSS Framework](https://github.com/discursus-io/discursus_core) instructions for how to use a library with the framework.

&nbsp;


# Configuring this library
The library requires you pass configruations in the form of a yaml file. 

Create a gdelt configuration file (`gdelt_configs.yaml`) in the `configs` section of the core framework.

```
resources:
  gdelt:
    config:
      event_code: 14 #You need to define at least an event code that you're targeting
      countries: #You can define 0 or more countries to target
        - US
        - CA
```

The example below includes 2 key-value pairs:
- `event_code` (required): We use that value to filter which events are to be mined from GDELT
- `countries`: We use those values to filter from which countries events are to be mined.

&nbsp;

# Library Ops
The library includes the following ops.

## gdelt_mining_ops.get_url_to_latest_asset
Op to fetch the latest url of GDELT asset

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