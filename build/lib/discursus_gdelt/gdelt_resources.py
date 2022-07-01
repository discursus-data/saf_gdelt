from dagster import resource, IntSource, Field

class GDELTClient:
    def __init__(self, event_code, countries):
        self._event_code = event_code
        self._countries = countries


    def get_event_code(self):
        return self._event_code


    def get_countries(self):
        return self._countries


@resource(
    config_schema={
        "resources": {
            "gdelt": {
                "config": {
                    "event_code": IntSource,
                    "countries": Field(list)
                }
            }
        }
    },
    description="A GDELT client.",
)
def gdelt_client(context):
    return GDELTClient(
        event_code = context.resource_config["resources"]["gdelt"]["config"]["event_code"],
        countries = context.resource_config["resources"]["gdelt"]["config"]["countries"]
    )