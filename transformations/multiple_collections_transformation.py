from typing import Any
from pymongo import MongoClient
from transformations.interfaces import ITransformation, IMongoResult
from sql_utils.database import Database


class MultipleCollectionsTransformation(ITransformation):

    def transform(self, db: Database) -> IMongoResult:
        db_dict = {}
        table_map = {t.name: t for t in db.tables}
        linking_tables = [t for t in db.tables if self.is_linking_table(t)]
        linking_table_names = {t.name for t in linking_tables}

        # Map of embedding instructions: {main_table: {field_name: {main_id: [linked_entity, ...]}}}
        embedding_map = {}

        for link_table in linking_tables:
            keys = list(link_table.entities[0].keys())
            id_fields = [k for k in keys if k.endswith("_id")]
            if len(id_fields) != 2:
                continue

            main_table = link_table.name.split("_")[0]
            main_id = [k for k in id_fields if k.startswith(main_table)][0]
            linked_id = [k for k in id_fields if k != main_id][0]
            field_name = self._field_name_from_id(linked_id)

            # Get full linked entity data from referenced table
            linked_table_name = linked_id.replace("_id", "")
            linked_entities = {e[linked_id]: e for e in table_map.get(linked_table_name, []).entities}

            for row in link_table.entities:
                main_val = row[main_id]
                linked_val = row[linked_id]
                linked_entity = linked_entities.get(linked_val)
                if linked_entity:
                    embedding_map.setdefault(main_table, {}).setdefault(field_name, {}).setdefault(main_val, []).append(linked_entity)

        for table in db.tables:
            table_name = table.name
            if table_name in linking_table_names:
                continue  # skip linking tables after embedding

            enriched_entities = []
            for entity in table.entities:
                pk_name = self._primary_key_name(entity)
                pk_val = entity[pk_name]
                for field_name, mapping in embedding_map.get(table_name, {}).items():
                    if pk_val in mapping:
                        entity[field_name] = mapping[pk_val]
                enriched_entities.append(entity)
            db_dict[table_name] = enriched_entities

        return MultipleCollectionsResult(db.name, db_dict)

    def is_linking_table(self, table) -> bool:
        if not table.entities:
            return False
        keys = list(table.entities[0].keys())
        id_fields = [k for k in keys if k.endswith("_id")]
        return len(id_fields) == 2 and len(keys) <= 3

    def _field_name_from_id(self, field: str) -> str:
        return field.replace("_id", "") + "s"

    def _id_field_name_from_field(self, plural: str) -> str:
        return plural[:-1] + "_id"

    def _primary_key_name(self, entity: dict) -> str:
        for k in entity:
            if k.endswith("_id"):
                return k
        raise ValueError("No primary key field ending with '_id' found.")


class MultipleCollectionsResult(IMongoResult):

    def __init__(self, db_name: str, content: dict) -> None:
        self._db_name = db_name
        self._content = content

    def persist(self, mongo_client: MongoClient) -> None:
        db = mongo_client[self._db_name + "_col"]
        for name, docs in self._content.items():
            collection = db[name]
            if docs:
                collection.insert_many(docs)
            else:
                db.create_collection(name)

