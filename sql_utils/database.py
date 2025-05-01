from __future__ import annotations

import os

from sql_utils.table import Table
from sql_utils.reader import SqlReader


class Database:

    def __init__(self, name: str, tables: list[Table]) -> None:
        self.name = name
        self.tables = tables

    def __repr__(self) -> str:
        s = ""
        for table in self.tables:
            s += str(table)
            s += "\n"

        return s

    @classmethod
    def from_file(cls, path: str) -> Database:
        reader = SqlReader(path)
        tables = reader.read()

        base_name = os.path.basename(path)
        db_name = os.path.splitext(base_name)[0]
        return Database(db_name, tables)
