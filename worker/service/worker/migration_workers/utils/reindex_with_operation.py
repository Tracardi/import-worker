from worker.domain.migration_schema import MigrationSchema
from worker.misc.update_progress import update_progress
from worker.misc.add_task import add_task
import requests
from worker.service.worker.migration_workers.utils.migration_error import MigrationError
import functools


def reindex_with_operation(func):
    @functools.wraps(func)
    def wrapper(celery_job, schema: MigrationSchema, url: str, task_index: str):
        add_task(
            url,
            task_index,
            f"Migration of \"{schema.copy_index.from_index}\"",
            celery_job,
            schema.dict()
        )

        try:
            doc_count = requests.get(
                f"{url}/{schema.copy_index.from_index}/_count",
                verify=False
            ).json()["count"]
            update_progress(celery_job, 0, doc_count)
            pagesize = 10
            moved_records = 0

            if schema.copy_index.script is None:
                schema.copy_index.script = ""

            while True:
                records_to_move = requests.get(
                    f"{url}/{schema.copy_index.from_index}/_search?from={moved_records}&size={pagesize}",
                    verify=False
                ).json()["hits"]["hits"]

                if not records_to_move:
                    break

                for number, record in enumerate(records_to_move):
                    record = func(celery_job, schema, url, task_index, record)

                    requests.post(f"{url}/{schema.copy_index.to_index}/_update/{record['_id']}",
                                  json={
                                      "scripted_upsert": True,
                                      "script": {
                                          "source": f"ctx._source = params.document;\n{schema.copy_index.script}",
                                          "params": {
                                              "document": record["_source"]
                                          }
                                      },
                                      "upsert": {}
                                  },
                                  verify=False)

                    update_progress(celery_job, moved_records + number + 1, doc_count)

                moved_records += pagesize

        except Exception as e:
            raise MigrationError(f"Index {schema.copy_index.from_index} could not be moved due to an error: {str(e)}")

    return wrapper
