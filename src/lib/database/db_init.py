import dataclasses
import uuid
from logging import Logger
from pathlib import Path
from typing import Iterable

from src.lib import utils
from src.lib.constants import DATABASE_PATH_TMP, XML_BASE_PATH
from src.lib.database import deduplicate
from src.lib.database.database import Database
from src.lib.database.sqlite.database_sqlite_impl import (DatabaseSQLiteImpl,
                                                          get_engine)
from src.lib.xml import tamer

log: Logger = utils.get_logger(__name__)


def db_init(db_path: str = DATABASE_PATH_TMP, files_base_path: str = XML_BASE_PATH) -> None:
    log.warning("DB Init started...")
    log.info(f"db: {db_path}, file base path: {files_base_path}")
    files = Path(files_base_path).rglob('*.xml')
    db = make_sqlite_db()
    populate_db(db, files)
    log.warning("DB Init finished.")


def make_sqlite_db(db_path: str = DATABASE_PATH_TMP) -> Database:
    log.info(f"Removing Database: {db_path}")
    Path(db_path).unlink(missing_ok=True)
    log.info(f"Creating Database: {db_path}")
    engine = get_engine(db_path)
    db = DatabaseSQLiteImpl(engine)
    log.info("Setting up Database")
    db.setup_db()
    log.info("Database set up")
    return db


def populate_db(db: Database, files: Iterable[Path]) -> None:
    ppl = tamer.get_ppl_names()
    catalogue_entries = tamer.get_metadata_from_files(files)
    catalogue_entries_unique = []
    ids_used = set()
    for e in catalogue_entries:
        cid = e.catalogue_id
        if not cid in ids_used:
            ids_used.add(cid)
            catalogue_entries_unique.append(e)
        else:
            uid = str(uuid.uuid4())
            new_e = dataclasses.replace(e, catalogue_id=uid)
            catalogue_entries_unique.append(new_e)
            print(f"WARNING: Duplicate Catalogue ID found: {cid} -> replaced by {uid}")
        # TODO: find a better solution - currently we have the following handrit IDs duplicated:
        # {'AM04-0737-II-en', 'AM04-0737-II-da', 'KG34-en', 'AM04-0004-en', 'AM04-0575-a-en', 'AM04-1058-da', 'SAM-0063-is', 'KG37-en'}
    manuscripts = deduplicate.get_unified_metadata(catalogue_entries_unique)
    db.add_data(ppl, catalogue_entries_unique, manuscripts)
