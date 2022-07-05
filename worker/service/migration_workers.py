import requests
from worker.domain.migration_schema import MigrationSchema
from worker.misc.update_progress import update_progress


def reindex(celery_job, schema: MigrationSchema, url: str):
    body = {
        "source": {
            "index": schema.copy_index.from_index
        },
        "dest": {
            "index": schema.copy_index.to_index
        }
    }
    if schema.copy_index.script is not None:
        body["script"] = {"lang": "painless", "source": schema.copy_index.script}

    with requests.post(
        url=f"{url}/_reindex",
        json=body
    ) as response:
        #print(response.json())
        #time.sleep(5)

        update_progress(celery_job, 100)
