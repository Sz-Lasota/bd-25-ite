from typing import Any

from pymongo import MongoClient

from transformations.interfaces import ITransformation, IMongoResult
from sql_utils.database import Database


class MultipleCollectionsTransformation(ITransformation):

    def transform(self, db: Database) -> IMongoResult:
        db_dict = {}
        for table in db.tables:
            db_dict[table.name] = table.entities

        return MultipleCollectionsResult(db.name, db_dict)


class MultipleCollectionsResult(IMongoResult):

    def __init__(self, db_name: str, content: dict) -> None:
        self._db_name = db_name
        self._content = content

    def persist(self, mongo_client: MongoClient) -> None:
        db = mongo_client[self._db_name + "_col"]
        for name in self._content:
            collection = db[name]
            if len(self._content[name]) > 0:
                collection.insert_many(self._content[name])
