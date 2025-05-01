from typing import Any

from pymongo import MongoClient

from transformations.interfaces import ITransformation, IMongoResult
from sql_utils.database import Database


class OneDocTransformation(ITransformation):

    def transform(self, db: Database) -> IMongoResult:
        doc_db = {}
        for table in db.tables:
            doc_db[table.name] = table.entities

        return OneDocResult(db.name, doc_db)


class OneDocResult(IMongoResult):

    def __init__(self, db_name: str, content: dict[str, Any]) -> None:
        self._db_name = db_name
        self._content = content

    def persist(self, mongo_client: MongoClient) -> None:
        db = mongo_client[self._db_name]
        collection = db[self._db_name]

        collection.insert_one(self._content)
