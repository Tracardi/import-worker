from typing import Optional
from elasticsearch import Elasticsearch
from pydantic import BaseModel

from worker.domain.named_entity import NamedEntity


class ElasticCredentials(BaseModel):
    url: str
    port: int
    scheme: str
    username: Optional[str] = None
    password: Optional[str] = None
    verify_certs: bool = True

    def has_credentials(self):
        return self.username is not None and self.password is not None


class ElasticImporter(BaseModel):
    index: NamedEntity
    batch: int

    def data(self, credentials: ElasticCredentials):

        if credentials.has_credentials():
            client = Elasticsearch(
                [credentials.url],
                http_auth=(credentials.username, credentials.password),
                scheme=credentials.scheme,
                port=credentials.port
            )
        else:
            client = Elasticsearch(
                [credentials.url],
                scheme=credentials.scheme,
                port=credentials.port
            )

        result = client.count(body={
            "query": {
                "match_all": {}
            }
        }, index=self.index.id)

        number_of_records = result['count']
        if number_of_records > 0:
            for batch_number, start in enumerate(range(0, number_of_records, self.batch)):
                query = {
                    "query": {
                        "match_all": {}
                    },
                    "from": start,
                    "size": self.batch
                }
                result = client.search(body=query, index=self.index.id)
                for record, data in enumerate(result['hits']['hits']):
                    data = data['_source']
                    progress = ((start + record + 1) / number_of_records) * 100
                    yield data, progress, batch_number + 1

        client.close()
