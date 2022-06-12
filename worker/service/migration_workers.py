import requests
from worker.domain.migration_schema import MigrationSchema
import time
from worker.misc.update_progress import update_progress


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

        #time.sleep(5)

        update_progress(celery_job, 100)
