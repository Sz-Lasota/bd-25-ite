import json

from pymongo import MongoClient

from sql_utils.database import Database
from transformations.one_document_transformation import OneDocTransformation

db_path = "./sakila.db"

if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017/")

    db = Database.from_file(db_path)
    trans_obj = OneDocTransformation()
    doc_db = trans_obj.transform(db)

    doc_db.persist(client)
