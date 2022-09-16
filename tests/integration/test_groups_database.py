from pathlib import Path
import sqlite3

from src.lib.database import groups_database
from src.lib.constants import DATABASE_GROUPS_PATH


def test_create_sqlite_connection_inmem() -> None:
    con = groups_database.create_connection(':memory:')
    assert isinstance(con, sqlite3.Connection)


def test_create_sqlite_connection_disk() -> None:
    con = groups_database.create_connection()
    assert isinstance(con, sqlite3.Connection)
    cur = con.cursor()
    cur.execute("PRAGMA database_list")
    res: tuple[int, str, str] = cur.fetchone()
    con.close()
    db_nr, db_name, db_file = res
    assert db_nr == 0
    assert db_name == 'main'
    assert db_file.endswith('groups.db')
    db_path = Path(db_file)
    expected_path = Path(DATABASE_GROUPS_PATH).absolute()
    assert db_path == expected_path
    assert db_path.exists()
