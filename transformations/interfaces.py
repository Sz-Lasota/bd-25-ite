from __future__ import annotations

from abc import ABC, abstractmethod

from pymongo import MongoClient

from sql_utils.database import Database


class ITransformation(ABC):

    @abstractmethod
    def transform(self, db: Database) -> IMongoResult: ...


class IMongoResult(ABC):

    @abstractmethod
    def persist(self, mongo_client: MongoClient) -> None: ...
