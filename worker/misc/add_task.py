import requests
from datetime import datetime
import logging


logger = logging.getLogger("logger")


def add_task(elastic_host: str, task_index: str, name: str, job, params=None):

    if params is None:
        params = {}

    with requests.post(
        url=f"{elastic_host}/{task_index}/_doc",
        json={
            "id": job.request.id,
            "name": name,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "status": "PROGRESS",
            "type": "upgrade",
            "progress": 0.,
            "event_type": "missing",
            "params": params,
            "task_id": job.request.id
        },
        verify=False
    ) as response:

        if response.status_code // 100 == 2:
            logger.info(msg=f"Successfully added task with ID {job.request.id}")

        else:
            logger.info(msg=f"Could not add task with ID {job.request.id} due to an error: {response.text}")

