REDIS_HOST=localhost ELASTIC_VERIFY_CERTS=no ELASTIC_HOST=https://elastic:NX499fCnF3431F9a6xoC0mQU@192.168.1.190:9200 celery -A worker.celery_worker worker -l info -E 

# Shows queues

REDIS_HOST=192.168.1.101 celery -A worker.celery_worker inspect registered


# GUI

pip install flower
REDIS_HOST=192.168.1.101 celery -A worker.celery_worker flower


# Troubleshooting

Make sure that redis host in Tracardi is the same as redis in Worker.

Common mistake:

In tracardi
REDIS_HOST = http://url:port/1

In worker
REDIS_HOST = http://url:port