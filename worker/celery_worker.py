import time

from celery import Celery
from .config import redis_config

celery = Celery(
    __name__,
    broker=redis_config.get_redis_with_password(),
    backend=redis_config.get_redis_with_password()
)


@celery.task(bind=True)
def run_celery_replay_job(self, config, credentials):
    for x in range(0, 1000):
        self.update_state(state="PROGRESS", meta={'current': x/100, 'total': 100})
        time.sleep(.5)
