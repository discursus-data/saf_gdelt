# discursus_gdelt
GDELT mining package for the discursus core project

# How to use this package
## Core Framework
- [Use the core framework](https://github.com/discursus-io/discursus_core)
- install the library in your Docker file: `RUN pip3 install git+https://github.com/discursus-io/discursus_gdelt@release/0.1`


## Configurations
### Configure the Package Resource
Create a gdelt configuration file (`gdelt_configs.yamls`) in the `configs` section of the core framwork.

```
resources:
  gdelt:
    config:
      event_code: 14 #You need to define at least an event code that you're targeting
      countries: #You can define 0 or more countries to target
        - US
        - CA
```

### Configure the AWS Resource
Create a aws configuration file (`aws_configs.yamls`) in the `configs` section of the core framwork.

```
resources:
  s3:
    config:
      bucket_name: discursus-io
```

Create a AWS resource file (`aws_resource.py`) in the `resources` section of the core framework.

```
from dagster import resource, StringSource

class AWSClient:
    def __init__(self, s3_bucket_name):
        self._s3_bucket_name = s3_bucket_name


    def get_s3_bucket_name(self):
        return self._s3_bucket_name


@resource(
    config_schema={
        "resources": {
            "s3": {
                "config": {
                    "bucket_name": StringSource
                }
            }
        }
    },
    description="A AWS client.",
)
def aws_client(context):
    return AWSClient(
        s3_bucket_name = context.resource_config["resources"]["s3"]["config"]["bucket_name"]
    )
```

## Calling a function
When you call function (a Dagster op) from the library, you will need to pass the resources you configured.

```
aws_configs = config_from_files(['configs/aws_configs.yaml'])
gdelt_configs = config_from_files(['configs/gdelt_configs.yaml'])

my_aws_client = aws_client.configured(aws_configs)
my_gdelt_client = gdelt_resources.gdelt_client.configured(gdelt_configs)

@job(
    resource_defs = {
        'aws_client': my_aws_client,
        'gdelt_client': my_gdelt_client
    }
)
def mine_gdelt_data():
    latest_gdelt_events_s3_location = gdelt_mining_ops.mine_gdelt_events()
```


# Development of library
- Once improvements have been added to package
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch