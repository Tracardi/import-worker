import requests

from worker.service.mysql.domain.config import MySQLImporter, MysqlConnectionConfig


class MySqlImportManager:

    def __init__(self, credentials: MysqlConnectionConfig, importer: MySQLImporter, webhook_url: str):
        self.importer = importer
        self.webhook_url = webhook_url
        self.credentials = credentials

    def run(self, tracardi_api_url):
        if tracardi_api_url[-1] == '/':
            tracardi_api_url = tracardi_api_url[:-1]
        for data, progress, batch in self.importer.data(self.credentials, self.importer.batch):
            url = f"{tracardi_api_url}{self.webhook_url}"
            response = requests.post(url, json=data)
            # print(data)
            print(url, response.json())
            yield progress, batch

