import requests
from worker.domain.migration_schema import MigrationSchema


def reindex(celery_job, schema: MigrationSchema, url: str):
    body = {
        "source": {
            "index": schema.from_index
        },
        "dest": {
            "index": schema.to_index
        }
    }
    if schema.script is not None:
        body["script"] = {"lang": "painless", "source": schema.script}

    with requests.post(
        url=f"{url}/_reindex",
        json=body
    ) as response:

        pass

