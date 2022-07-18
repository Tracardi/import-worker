import requests
from worker.domain.migration_schema import MigrationSchema
from worker.misc.update_progress import update_progress
from worker.misc.add_task import add_task
import logging
from time import sleep


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
        url=f"{url}/_reindex?wait_for_completion=false",
        json=body
    ) as response:

        if response.status_code != 200:
            raise MigrationError(response.text)

        task_id = response.json()["task"]
        while True:
            task_response = requests.get(f"{url}/_tasks/{task_id}")
            if task_response.status_code != 200 or task_response.json()["completed"] is True:
                break
            status = task_response.json()["task"]["status"]
            update_progress(celery_job, status["updated"] + status["created"], status["total"])
            sleep(3)

        logging.info(f"Migration from `{schema.copy_index.from_index}` to `{schema.copy_index.to_index}` complete.")

        update_progress(celery_job, 100)


def user_reindex(celery_job, schema: MigrationSchema, url: str, task_index: str):
    add_task(
        url,
        task_index,
        f"Migration of \"{schema.copy_index.from_index}\"",
        celery_job,
        schema.dict()
    )

    try:
        doc_count = requests.get(f"{url}/{schema.copy_index.from_index}/_count").json()["count"]
        update_progress(celery_job, 0, doc_count)
        pagesize = 10
        moved_records = 0

        while True:
            records_to_move = requests.get(
                f"{url}/{schema.copy_index.from_index}/_search?from={moved_records}&size={pagesize}"
            ).json()["hits"]["hits"]

            if not records_to_move:
                break

            for number, record in enumerate(records_to_move):
                user_exists = requests.get(
                    f"{url}/{schema.copy_index.to_index}/_doc/{record['_id']}"
                ).status_code == 200

                record = {key: record["_source"][key] for key in record["_source"] if key != "token"}

                if not user_exists:
                    record["token"] = None
                    requests.post(f"{url}/{schema.copy_index.to_index}/_create/{record['id']}", json=record)

                else:
                    requests.post(f"{url}/{schema.copy_index.to_index}/_update/{record['id']}", json={"doc": record})

                update_progress(celery_job, moved_records + number + 1, doc_count)

            moved_records += pagesize

    except Exception as e:
        raise MigrationError(f"User index could not be moved due to an error: {str(e)}")
