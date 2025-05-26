from typing import Any

from pymongo import MongoClient

from transformations.interfaces import ITransformation, IMongoResult
from sql_utils.database import Database


class MultipleDocsTransformation(ITransformation):

    def transform(self, db: Database) -> IMongoResult:
        db_list = []
        for table in db.tables:
            doc_db = {"name": table.name}
            i = 0
            for ent in table.entities:
                doc_db[str(i)] = ent
                i += 1
            db_list.append(doc_db)

        return MultipleDocsResult(db.name, db_list)


class MultipleDocsResult(IMongoResult):

    def __init__(self, db_name: str, content: list) -> None:
        self._db_name = db_name
        self._content = content

    def persist(self, mongo_client: MongoClient) -> None:
        db = mongo_client[self._db_name + "_new"]
        collection = db[self._db_name]

        collection.insert_many(self._content)
