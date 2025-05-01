import sqlite3
from sql_utils.table import Table


class SqlReader:

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

        try:
            self._connection = sqlite3.connect(db_path)
        except sqlite3.Error as e:
            raise RuntimeError(f"Cannot connect to database {e}")

    def read(self) -> list[Table]:
        names = self._get_all_tables()
        return [self._read_table_data(name) for name in names]

    def _get_all_tables(self) -> list[str]:
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
        except sqlite3.Error as e:
            raise RuntimeError(f"Error fetching table names: {e}")

        return table_names

    def _read_table_data(self, table_name) -> Table:
        try:
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT * FROM {table_name};")
            rows = cursor.fetchall()

            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [info[1] for info in cursor.fetchall()]
        except sqlite3.Error as e:
            raise RuntimeError(f"Error reading data from table {table_name}: {e}")

        return Table(table_name, columns, rows)
