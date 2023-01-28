import dataclasses
import uuid
from logging import Logger
from pathlib import Path
from typing import Iterable

from src.lib import utils
from src.lib.constants import DATABASE_PATH, XML_BASE_PATH
from src.lib.database import deduplicate
from src.lib.database.database import Database
from src.lib.database.sqlite.database_sqlite_impl import (DatabaseSQLiteImpl,
                                                          get_engine)
from src.lib.xml import tamer

log: Logger = utils.get_logger(__name__)


def db_init(db_path: str = DATABASE_PATH, files_base_path: str = XML_BASE_PATH) -> None:
    """Initialize and populate the database, provided the DB path and the base path where the XML files are located"""
    log.warning("DB Init started...")
    log.info(f"db: {db_path}, file base path: {files_base_path}")
    files = Path(files_base_path).rglob('*.xml')
    db = make_sqlite_db()
    populate_db(db, files)
    log.warning("DB Init finished.")


def make_sqlite_db(db_path: str = DATABASE_PATH) -> Database:
    """Remove the old DB file, create a new one and add all tables to it."""
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
    """Extract all data from the XML files and add it to the database."""
    ppl = tamer.get_ppl_names()
    log.info(f"Loaded people information: {len(ppl)}")
    catalogue_entries = tamer.get_metadata_from_files(files)
    log.info(f"Loaded catalogue entries: {len(catalogue_entries)}")
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
            log.warning(f"Duplicate Catalogue ID found: {cid} -> replaced by {uid}")
    log.info("Ensured that catalogue IDs are unique")
    manuscripts = deduplicate.get_unified_metadata(catalogue_entries_unique)
    log.info(f"Deduplicated catalogue entries to manuscript metadata: {len(manuscripts)}")
    db.add_data(ppl, catalogue_entries_unique, manuscripts)
    log.info("Added all data to DB.")
