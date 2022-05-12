import requests

from worker.service.mysql.domain.config import MySQLImporter, MysqlConnectionConfig


class MySqlImportManager:

    def __init__(self, connection: MysqlConnectionConfig, importer: MySQLImporter, webhook_url: str):
        self.importer = importer
        self.webhook_url = webhook_url
        self.db = connection.connect()

    def run(self, tracardi_api_url):
        if tracardi_api_url[-1] == '/':
            tracardi_api_url = tracardi_api_url[:-1]
        for data, progress, batch in self.importer.data(self.db, self.importer.batch):
            print(data, progress, batch)
            response = requests.post(f"{tracardi_api_url}{self.webhook_url}", json=data)
            print(response.json())

    def __del__(self):
        if self.db:
            self.db.close()
