# discursus_gdelt
GDELT mining package for the discursus core project

# How to use this package
- [Use the core framework](https://github.com/discursus-io/discursus_core)
- install the library in your Docker file: `RUN pip3 install git+https://github.com/discursus-io/discursus_gdelt@release/0.1`


# How to configure this package
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
        """
        Returns the connection used by the resource for querying data.
        Should in principle not be used directly.
        """

        if self._session is None:
            self._base_url = self._conn_host

            # Build our session instance, which we will use for any
            # requests to the API.
            self._session = requests.Session()

            self._session.auth = (self._conn_login, self._conn_password)

        return self._session, self._base_url


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

Once you call an op from the library, you will need to pass the AWS resource you created.

```
@job(
    resource_defs = {
        'aws_client': my_aws_client
    }
)
def mine_gdelt_data():
    latest_gdelt_events_s3_location = gdelt_mining_ops.mine_gdelt_events()
```


# Instructions to build library
- Once improvements have been added to package
- Compile a new version: `python setup.py bdist_wheel`
- Commit branch and PR into new release branch
- Point projects to new release branch