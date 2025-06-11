from pymongo import MongoClient

from sql_utils.database import Database
from transformations.interfaces import ITransformation

class TransformationRunner:

    def __init__(self, transformations: dict[str, ITransformation]):
        self._transformations = transformations
        self._mongo_client: MongoClient | None = None

    def modes(self) -> list[str]:
        return list(self._transformations.keys())

    def configure_mongo_client(self, connection_str: str) -> None:
        self._mongo_client = MongoClient(connection_str)

    def run_transformation(self, mode: str, db_path: str) -> None:
        db = Database.from_file(db_path)

        if mode not in self._transformations.keys():
            raise ValueError(f"Invalid transformation: {mode}")
        transformation = self._transformations[mode]
        result = transformation.transform(db)

        result.persist(self._mongo_client)
