import tkinter as tk

from pymongo import MongoClient

from presentation.ui import DatabaseGUI
from sql_utils.database import Database
from transformations.multiple_docs_transformation import MultipleDocsTransformation
from transformations.one_document_transformation import OneDocTransformation
from transformations.multiple_collections_transformation import MultipleCollectionsTransformation
from transformations.runner import TransformationRunner

# Mongo conn_str: mongodb://localhost:27017/

if __name__ == "__main__":
    runner = TransformationRunner({
        "To one document": OneDocTransformation(),
        "To multiple documents": MultipleDocsTransformation(),
        "To multiple collections": MultipleCollectionsTransformation(),
    })

    root = tk.Tk()
    app = DatabaseGUI(root, runner)
    root.mainloop()
