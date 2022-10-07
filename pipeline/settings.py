import pathlib
import sqlite3

DATABASE_FILE = pathlib.Path(__file__).parent.parent / "pipeline.db"

db_connection = sqlite3.connect(DATABASE_FILE)
