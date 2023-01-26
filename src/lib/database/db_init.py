import sqlite3
from logging import Logger
import streamlit as st
from src.lib import utils
from src.lib.database.deduplicate import UnifiedMetadata
from src.lib.xml.tamer import MetadataRowType

# @staticmethod
# def _build_db() -> None:
#     with database.create_connection(":memory:") as db_conn:
#         db_init.db_set_up(db_conn)
#         ppl = tamer.get_ppl_names()
#         db_init.populate_people_table(db_conn, ppl)
#         files = Path(XML_BASE_PATH).rglob('*.xml')
#         # files = list(Path(XML_BASE_PATH).rglob('*.xml'))[:100]
#         ms_meta, msppl, mstxts = tamer.get_metadata_from_files(files)
#         db_init.populate_ms_table(db_conn, ms_meta)
#         ms_ppl = [x for y in msppl for x in y if x[2] != 'N/A']
#         ms_txts = [x for y in mstxts for x in y if x[2] != "N/A"]  # TODO-BL: I'd like to get rid of "N/A"
#         db_init.populate_junction_pxm(db_conn, ms_ppl)
#         db_init.populate_junction_txm(db_conn, ms_txts)
#         unified_metadata = deduplicate.get_unified_metadata(ms_meta)
#         db_init.populate_unified_ms_table(db_conn, unified_metadata)
#         db_init.populate_junction_pxm_unified(db_conn, ms_ppl)
#         db_init.populate_junction_txm_unified(db_conn, ms_txts)
#         with database.create_connection(DATABASE_PATH) as dest_conn:
#             db_conn.backup(dest_conn)


@st.experimental_singleton
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


def db_set_up(conn: sqlite3.Connection) -> None:
    '''This function creates all the tables for the SQLite DB and
    defines the schema.

    Args:
        conn: SQLite Connection object

    Returns:
        None
    '''
    log.info("Setting up database tables...")
    curse = conn.cursor()
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS people (
            persID PRIMARY KEY,
            firstName,
            lastName
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS manuscripts (
            shelfmark,
            shorttitle,
            country,
            settlement,
            repository,
            origin,
            date,
            terminusPostQuem,
            terminusAnteQuem,
            meandate,
            yearrange,
            support,
            folio,
            height,
            width,
            extent,
            description,
            creator,
            id,
            full_id PRIMARY KEY,
            filename
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS manuscriptUnified (
            handrit_id TEXT PRIMARY KEY,
            shelfmark TEXT,
            catalogue_entries INTEGER,
            ms_title TEXT,
            country TEXT,
            settlement TEXT,
            repository TEXT,
            origin TEXT,
            full_date TEXT,
            terminus_post_quem INTEGER,
            termini_post_quos TEXT,
            terminus_ante_quem INTEGER,
            termini_ante_quos TEXT,
            mean_date INTEGER,
            date_standard_deviation REAL,
            support TEXT,
            folio INTEGER,
            height TEXT,
            width TEXT,
            extent TEXT,
            description TEXT,
            creator TEXT,
            full_ids TEXT,
            file_names TEXT
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS junctionPxM (
            locID INTEGER PRIMARY KEY AUTOINCREMENT,
            persID,
            msID,
            FOREIGN KEY(persID) REFERENCES people(persID) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(msID) REFERENCES manuscripts(full_id) ON DELETE CASCADE ON UPDATE CASCADE
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS junctionTxM (
            locID INTEGER PRIMARY KEY AUTOINCREMENT,
            msID,
            txtName,
            FOREIGN KEY(msID) REFERENCES manuscripts(full_id) ON DELETE CASCADE ON UPDATE CASCADE
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS junctionPxMU (
            locID INTEGER PRIMARY KEY AUTOINCREMENT,
            persID,
            handritID,
            UNIQUE(persID, handritID),
            FOREIGN KEY(persID) REFERENCES people(persID) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(handritID) REFERENCES manuscripts(id) ON DELETE CASCADE ON UPDATE CASCADE
        )'''
    )
    curse.execute(
        '''CREATE TABLE IF NOT EXISTS junctionTxMU (
            locID INTEGER PRIMARY KEY AUTOINCREMENT,
            handritID,
            txtName,
            UNIQUE(handritID, txtName),
            FOREIGN KEY(handritID) REFERENCES manuscripts(id) ON DELETE CASCADE ON UPDATE CASCADE
        )'''
    )
    conn.commit()
    curse.close()
    log.info("Successfully created all database tables.")


def populate_people_table(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
    '''This function will populate the 'people' table with information about
    persons from the Handrit names authority file.
    Currently, the following information is held (in that order): First name, last name, unique handrit ID

    Args:
        conn (sqlite.Connection): DB connection object
        incoming (List[Tuple[str, str, str]]): handritID, first name, last name of persons to be stored
    '''
    curse = conn.cursor()
    curse.executemany('''INSERT OR IGNORE INTO people VALUES (?, ?, ?)''', incoming)
    conn.commit()
    curse.close()
    log.info(f"Successfully added people to people database table: {len(incoming)} entries.")


def populate_unified_ms_table(conn: sqlite3.Connection, incoming: list[UnifiedMetadata]) -> None:
    cursor = conn.cursor()
    data = [d.to_tuple() for d in incoming]
    query = f"INSERT OR IGNORE INTO manuscriptUnified VALUES ({', '.join('?' * 24)})"
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    log.info(f"Successfully added unified manuscript metadata into manuscript_unified: {len(incoming)} entries.")


def populate_ms_table(conn: sqlite3.Connection, incoming: list[MetadataRowType]) -> None:
    '''Function to populate the manuscripts table with data.

    Args:
        conn(sqlite3.Connection): DB connection object
        incoming(pd.DataFrame): Dataframe containing the manuscript data. Column names
        of dataframe must match column names of db table.

    Returns:
        None
    '''
    curse = conn.cursor()
    sql_query = '''INSERT OR IGNORE INTO manuscripts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    curse.executemany(sql_query, incoming)
    conn.commit()
    curse.close()
    log.info(f"Successfully added manuscripts to manuscript database table: {len(incoming)} entries.")


def populate_junction_pxm(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
    curse = conn.cursor()
    data = [(p, ms) for (ms, _, p) in incoming]
    curse.executemany('''INSERT OR IGNORE INTO junctionPxM(persID, msID) VALUES (?, ?)''', data)
    conn.commit()
    curse.close()
    log.info("Successfully populated PxM junction table.")


def populate_junction_txm(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
    curse = conn.cursor()
    data = [(ms, txt) for (ms, _, txt) in incoming]
    curse.executemany("INSERT OR IGNORE INTO junctionTxM(msID, txtName) VALUES (?,?)", data)
    conn.commit()
    curse.close()
    log.info("Successfully populated TxM junction table.")


def populate_junction_pxm_unified(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
    curse = conn.cursor()
    data = [(p, ms) for (_, ms, p) in incoming]
    curse.executemany('''INSERT OR IGNORE INTO junctionPxMU(persID, handritID) VALUES (?, ?)''', data)
    conn.commit()
    curse.close()
    log.info("Successfully populated PxM unified junction table.")


def populate_junction_txm_unified(conn: sqlite3.Connection, incoming: list[tuple[str, str, str]]) -> None:
    curse = conn.cursor()
    data = [(ms, txt) for (_, ms, txt) in incoming]
    curse.executemany("INSERT OR IGNORE INTO junctionTxMU(handritID, txtName) VALUES (?,?)", data)
    conn.commit()
    curse.close()
    log.info("Successfully populated TxM unified junction table.")


def pxm_integrity_check(conn: sqlite3.Connection, incoming: list[tuple[int, str, str]]) -> bool:
    curse = conn.cursor()
    curse.execute(f"SELECT persID FROM junctionPxM")
    l0 = [x[1] for x in incoming]
    l1 = []
    for row in curse.fetchall():
        l1.append(row[0])
    l2 = [x for x in l0 if x not in l1]
    l0.sort()
    l2.sort()
    log.info("Performing integrity check")
    check = l0 == l2
    return check
