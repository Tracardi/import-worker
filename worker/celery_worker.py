from celery import Celery
from worker.config import redis_config
from worker.domain.named_entity import NamedEntity
from worker.service.worker.elastic_worker import ElasticImporter, ElasticCredentials
from worker.service.worker.mysql_worker import MysqlConnectionConfig, MySQLImporter
from worker.service.import_dispatcher import ImportDispatcher
from worker.domain.import_config import ImportConfig

celery = Celery(
    __name__,
    broker=redis_config.get_redis_with_password(),
    backend=redis_config.get_redis_with_password()
)


def import_mysql_data(celery_job, import_config, credentials):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{import_config.event_source.id}"

    importer = ImportDispatcher(MysqlConnectionConfig(**credentials),
                                importer=MySQLImporter(**import_config.config),
                                webhook_url=webhook_url)

    for progress, batch in importer.run(import_config.api_url):
        if celery_job:
            celery_job.update_state(state="PROGRESS", meta={'current': progress, 'total': 100})


def import_elastic_data(celery_job, import_config, credentials):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{import_config.event_source.id}"

    importer = ImportDispatcher(ElasticCredentials(**credentials),
                                importer=ElasticImporter(**import_config.config),
                                webhook_url=webhook_url)

    for progress, batch in importer.run(import_config.api_url):
        if celery_job:
            celery_job.update_state(state="PROGRESS", meta={'current': progress, 'total': 100})


@celery.task(bind=True)
def run_mysql_import_job(self, import_config, credentials):
    import_mysql_data(self, import_config, credentials)


@celery.task(bind=True)
def run_elastic_import_job(self, import_config, credentials):
    import_elastic_data(self, import_config, credentials)


if __name__ == "__main__":
    import_elastic_data(
        celery_job=None,
        import_config={
            "name": 'tesst',
            "description": "desc",
            "api_url": "http://localhost:8686",
            "event_source": NamedEntity(
                id="@test-source",
                name="test"
            ).dict(),
            "event_type": "import-es",
            "module": "mod",
            "config": {
                "index": NamedEntity(id="tracardi-log-2022-5", name="mysql").dict(),
                "batch": 2
            },
            "enabled": True,
            "transitional": False
        },
        credentials=ElasticCredentials(
            url='localhost',
            scheme='http',
            port=9200
        ).dict()
    )

# if __name__ == "__main__":
#     import_mysql_data(
#         celery_job=None,
#         import_config={
#             "name": 'tesst',
#             "description": "desc",
#             "api_url": "http://localhost:8686",
#             "event_source": NamedEntity(
#                 id="@test-source",
#                 name="test"
#             ).dict(),
#             "event_type": "import",
#             "module": "mod",
#             "config": {
#                 "database_name": NamedEntity(id="mysql", name="mysql").dict(),
#                 "table_name": NamedEntity(id="time_zone", name="time_zone").dict(),
#                 "batch": 100
#             },
#             "enabled": True,
#             "transitional": False
#         },
#         credentials=MysqlConnectionConfig(
#             user='root',
#             password='root',
#             host='192.168.1.103',
#             port=3306
#         ).dict()
#     )
