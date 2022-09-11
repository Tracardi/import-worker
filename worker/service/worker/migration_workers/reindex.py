from worker.domain.migration_schema import MigrationSchema
from worker.misc.update_progress import update_progress
from worker.misc.add_task import add_task
from time import sleep
from worker.service.worker.migration_workers.utils.migration_error import MigrationError
import logging
from worker.service.worker.migration_workers.utils.client import ElasticClient


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

    with ElasticClient(hosts=[url]) as client:

        response = client.reindex(body=body)

        if not isinstance(response, dict) or "task" not in response:
            raise MigrationError(str(response))

        task_id = response["task"]
        while True:
            task_response = client.get_task(task_id)
            if task_response is None or task_response["completed"] is True:
                break
            status = task_response["task"]["status"]
            update_progress(celery_job, status["updated"] + status["created"], status["total"])
            sleep(3)

        logging.info(f"Migration from `{schema.copy_index.from_index}` to `{schema.copy_index.to_index}` complete.")

        update_progress(celery_job, 100)
