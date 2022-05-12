import time

from celery import Celery
from worker.config import redis_config
from worker.domain.named_entity import NamedEntity
from worker.service.mysql.domain.config import MysqlConnectionConfig, MySQLImporter
from worker.service.mysql_importer import MySqlImportManager
from worker.domain.import_config import ImportConfig

celery = Celery(
    __name__,
    broker=redis_config.get_redis_with_password(),
    backend=redis_config.get_redis_with_password()
)


def import_mysql_data(celery_job, import_config, credentials, source_id, tracardi_api_url):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{source_id}"

    importer = MySqlImportManager(MysqlConnectionConfig(**credentials),
                                  importer=MySQLImporter(**import_config.config),
                                  webhook_url=webhook_url)

    for progress, batch in importer.run(tracardi_api_url):
        if celery_job:
            celery_job.update_state(state="PROGRESS", meta={'current': progress, 'total': 100})

    importer.close()


@celery.task(bind=True)
def run_celery_import_job(self, import_config, credentials, source_id, tracardi_api_url):
    import_mysql_data(self, import_config, credentials, source_id, tracardi_api_url)


if __name__ == "__main__":
    import_mysql_data(
        celery_job=None,
        import_config={
        "name": 'tesst',
        "description": "desc",
        "event_type": "import",
        "module": "mod",
        "config": {
            "database_name": NamedEntity(id="mysql", name="mysql").dict(),
            "table_name": NamedEntity(id="db", name="db").dict(),
            "batch": 100
        },
        "enabled": True,
        "transitional": False
    },
        credentials=MysqlConnectionConfig(
            user='root',
            password='root',
            host='localhost',
            port=3306
        ).dict(),
        source_id="a8698bd6-88d5-4263-80b7-193a79a5019b",
        tracardi_api_url="http://localhost:8686/"
    )
