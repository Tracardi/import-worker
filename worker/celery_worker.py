from celery import Celery, group
from worker.config import redis_config
from worker.domain.named_entity import NamedEntity
from worker.service.worker.elastic_worker import ElasticImporter, ElasticCredentials
from worker.service.worker.mysql_worker import MysqlConnectionConfig, MySQLImporter
from worker.service.worker.mysql_query_worker import MysqlConnectionConfig as MysqlQueryConnConfig, MySQLQueryImporter
from worker.service.import_dispatcher import ImportDispatcher
from worker.domain.import_config import ImportConfig
from worker.domain.migration_schema import MigrationSchema
import logging
import worker.service.migration_workers as migration_workers
from worker.misc.update_progress import update_progress


celery = Celery(
    __name__,
    broker=redis_config.get_redis_with_password(),
    backend=redis_config.get_redis_with_password()
)

logger = logging.getLogger("logger")


def import_mysql_table_data(celery_job, import_config, credentials):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{import_config.event_source.id}"

    importer = ImportDispatcher(MysqlConnectionConfig(**credentials),
                                importer=MySQLImporter(**import_config.config),
                                webhook_url=webhook_url)

    for progress, batch in importer.run(import_config.api_url):
        update_progress(celery_job, progress)


def import_elastic_data(celery_job, import_config, credentials):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{import_config.event_source.id}"

    importer = ImportDispatcher(ElasticCredentials(**credentials),
                                importer=ElasticImporter(**import_config.config),
                                webhook_url=webhook_url)

    for progress, batch in importer.run(import_config.api_url):
        update_progress(celery_job, progress)


def import_mysql_data_with_query(celery_job, import_config, credentials):
    import_config = ImportConfig(**import_config)
    webhook_url = f"/collect/{import_config.event_type}/{import_config.event_source.id}"

    importer = ImportDispatcher(
        MysqlQueryConnConfig(**credentials),
        importer=MySQLQueryImporter(**import_config.config),
        webhook_url=webhook_url
    )

    for progress, batch in importer.run(import_config.api_url):
        update_progress(celery_job, progress)


def migrate_data(celery_job, schemas, elastic_host):
    logger.log(level=logging.INFO, msg="running_migration")
    schemas = [MigrationSchema(**schema) for schema in schemas]
    total = len(schemas)
    progress = 0

    update_progress(celery_job, progress, total)

    sync_chain = None
    jobs = []
    for schema in schemas:
        if schema.asynchronous is True:
            jobs.append((
                f"Moving data from {schema.from_index} to {schema.to_index}",
                run_migration_worker.delay(schema.worker, schema.dict(), elastic_host)
            ))

        else:
            sync_chain = run_migration_worker.s(schema.worker, schema.dict(), elastic_host) if sync_chain is None else \
                sync_chain | run_migration_worker.s(schema.worker, schema.dict(), elastic_host)

        progress += 1
        if celery_job:
            celery_job.update_state(state="PROGRESS", meta={"current": progress, "total": total})

    if sync_chain is not None:
        jobs.append(("Required synchronous migration tasks", sync_chain.delay()))

    return [(job_info[0], job_info[1].id) for job_info in jobs]


@celery.task(bind=True)
def run_mysql_import_job(self, import_config, credentials):
    import_mysql_table_data(self, import_config, credentials)


@celery.task(bind=True)
def run_elastic_import_job(self, import_config, credentials):
    import_elastic_data(self, import_config, credentials)


@celery.task(bind=True)
def run_mysql_query_import_job(self, import_config, credentials):
    import_mysql_data_with_query(self, import_config, credentials)


@celery.task(bind=True)
def run_migration_job(self, schemas, elastic_host):
    return migrate_data(self, schemas, elastic_host)


@celery.task(bind=True)
def run_migration_worker(self, worker_func, schema, elastic_host):
    try:
        worker_function = getattr(migration_workers, worker_func)

    except AttributeError:
        logger.log(level=logging.ERROR, msg=f"No migration worker defined for name {schema.worker}. "
                                            f"Skipping migration with name {schema.name}")
        return

    worker_function(self, MigrationSchema(**schema), elastic_host)


if __name__ == "__main__":
    import_mysql_data_with_query(
        celery_job=None,
        import_config={
            "name": "test",
            "description": "desc",
            "api_url": "http://localhost:8686",
            "event_source": NamedEntity(
                id="@test-source",
                name="test"
            ).dict(),
            "event_type": "import-mysql-query",
            "module": "mod",
            "config": {
                "index": NamedEntity(id="tracardi-log-2022-5", name="mysql").dict(),
                "batch": 2,
                "database_name": {"id": "Rfam", "name": "Rfam"},
                "query": "SELECT * FROM family WHERE match_pair_node=false",
            },
            "enabled": True,
            "transitional": False
        },
        credentials=MysqlQueryConnConfig(
            host="mysql-rfam-public.ebi.ac.uk",
            user="rfamro",
            password=None,
            port=4497
        ).dict()
    )


#if __name__ == "__main__":
#    import_elastic_data(
#        celery_job=None,
#        import_config={
#            "name": 'tesst',
#            "description": "desc",
#            "api_url": "http://localhost:8686",
#            "event_source": NamedEntity(
#                id="@test-source",
#                name="test"
#            ).dict(),
#            "event_type": "import-es",
#            "module": "mod",
#            "config": {
#                "index": NamedEntity(id="tracardi-log-2022-5", name="mysql").dict(),
#                "batch": 2
#            },
#            "enabled": True,
#            "transitional": False
#        },
#        credentials=ElasticCredentials(
#            url='localhost',
#            scheme='http',
#            port=9200
#        ).dict()
#    )

# if __name__ == "__main__":
#     import_mysql_table_data(
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
