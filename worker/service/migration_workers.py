import requests
from worker.domain.migration_schema import MigrationSchema
from worker.misc.update_progress import update_progress
from worker.misc.add_task import add_task
import logging
from hashlib import sha1
from datetime import datetime


class MigrationError(Exception):
    pass


def reindex(celery_job, schema: MigrationSchema, url: str, task_index: str):

    add_task(
        url,
        task_index,
        f"Migration of \"{schema.copy_index.from_index}\"",
        celery_job,
        schema.dict()
    )

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
        url=f"{url}/_reindex?wait_for_completion=true",
        json=body
    ) as response:

        if response.status_code != 200:
            raise MigrationError(response.text)

        logging.info(f"Migration from `{schema.copy_index.from_index}` to `{schema.copy_index.to_index}` complete.")

        update_progress(celery_job, 100)
