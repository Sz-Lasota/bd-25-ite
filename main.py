import json

from pymongo import MongoClient

from sql_utils.database import Database
from transformations.multiple_docs_transformation import MultipleDocsTransformation
from transformations.one_document_transformation import OneDocTransformation
from transformations.multiple_collections_transformation import MultipleCollectionsTransformation

db_path = "./sakila.db"

if __name__ == "__main__":
    a = input("1 - one document; 2 - one collection, many documents; 3 - many collections")
    client = MongoClient("mongodb://localhost:27017/")

    db = Database.from_file(db_path)
    if a == '1':
        trans_obj = OneDocTransformation()
    elif a == '2':
        trans_obj = MultipleDocsTransformation()
    else:
        trans_obj = MultipleCollectionsTransformation()
    doc_db = trans_obj.transform(db)

    doc_db.persist(client)
