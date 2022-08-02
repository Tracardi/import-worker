from worker.service.worker.migration_workers.utils.reindex_with_operation import reindex_with_operation
from worker.domain.migration_schema import MigrationSchema
import requests


@reindex_with_operation
def user_reindex(celery_job, schema: MigrationSchema, url: str, task_index: str, record: dict):
    user = requests.get(
        f"{url}/{schema.copy_index.to_index}/_doc/{record['_id']}",
        verify=False
    )
    user_exists = user.status_code == 200

    record["_source"] = {key: record["_source"][key] for key in record["_source"] if key != "token"}

    if user_exists:
        record["_source"]["token"] = user.json()["_source"]["token"]
    else:
        record["_source"]["token"] = None

    return record
