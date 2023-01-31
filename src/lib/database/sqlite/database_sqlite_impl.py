from dataclasses import dataclass, field
from logging import Logger
from uuid import UUID

import pandas as pd
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel, col, create_engine, select

from lib import utils
from lib.constants import DATABASE_PATH
from lib.database.sqlite.models import (CatalogueEntries, Groups,
                                        Manuscripts, People,
                                        PersonCatalogueJunction,
                                        PersonManuscriptJunction,
                                        TextCatalogueJunction,
                                        TextManuscriptJunction, Texts)
from lib.groups import Group, GroupType
from lib.manuscripts import CatalogueEntry, Manuscript
from lib.people import Person

log: Logger = utils.get_logger(__name__)


def get_engine(db_path: str = DATABASE_PATH) -> Engine:
    """Creates a SQLAlchemy Engine, given a DB path. Can be `:memory:` for an in-memory database."""
    log.info(f"Get DB Engine from: {db_path}")
    sqlite_url = f"sqlite:///{db_path}"
    return create_engine(sqlite_url)


@dataclass(frozen=True)
class DatabaseSQLiteImpl:
    """SQLite implementation of the `Database`protocol."""
    engine: Engine = field(default_factory=get_engine)

    def setup_db(self) -> None:
        log.info("Create Database Metadata")
        SQLModel.metadata.create_all(self.engine)
        log.info(f"Database has tables: {list(SQLModel.metadata.tables.keys())}")

    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        log.debug(f"Loading metadata for manuscripts: {ms_ids}")
        with Session(self.engine) as session:
            statement = select(Manuscripts).where(col(Manuscripts.manuscript_id).in_(ms_ids))
            mss = session.exec(statement).all()
            log.debug(f"Retrieved metadata entries: {len(mss)}")
            ms_dicts = [ms.dict() for ms in mss]
            return pd.DataFrame(ms_dicts)

    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        log.debug(f"Loading manuscripts by people: {pers_ids}")
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id).where(col(Manuscripts.people).any(col(People.pers_id).in_(pers_ids)))
            mss = session.exec(statement).all()
            log.debug(f"Retrieved manuscripts: {len(mss)}")
            return mss

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        log.debug(f"Loading people by manuscripts: {ms_ids}")
        with Session(self.engine) as session:
            statement = select(People.pers_id).where(col(People.manuscripts).any(col(Manuscripts.manuscript_id).in_(ms_ids)))
            ppl = session.exec(statement).all()
            log.debug(f"Retrieved people: {len(ppl)}")
            return ppl

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        log.debug(f"Loading manuscripts by texts: {txts}")
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id).where(col(Manuscripts.texts).any(col(Texts.text_id).in_(txts)))
            mss = session.exec(statement).all()
            log.debug(f"Retrieved manuscripts: {len(mss)}")
            return mss

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        log.debug(f"Loading texts by manuscripts: {ms_ids}")
        with Session(self.engine) as session:
            statement = select(Texts.text_id).where(col(Texts.manuscripts).any(col(Manuscripts.manuscript_id).in_(ms_ids)))
            txts = session.exec(statement).all()
            log.debug(f"Retrieved texts: {len(txts)}")
            return txts

    def persons_lookup_dict(self) -> dict[str, str]:
        with Session(self.engine) as session:
            ppl = session.exec(select(People)).all()
            res = {p.pers_id: f"{p.first_name} {p.last_name}" for p in ppl}
            log.info(f"Created person lookup dict: {len(res.keys())}")
            return res

    def ms_lookup_dict(self) -> dict[str, list[str]]:
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id, Manuscripts.shelfmark, Manuscripts.title)
            mss = session.exec(statement).all()
            res = {x[0]: [x[1], x[2]] for x in mss}
            log.info(f"Created manuscript lookup dict: {len(res.keys())}")
            return res

    def txt_lookup_list(self) -> list[str]:
        with Session(self.engine) as session:
            res = session.exec(select(Texts.text_id)).all()
            log.info(f"Created text lookup list: {len(res)}")
            return res

    def get_ms_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.ManuscriptGroup)
            groups = session.exec(statement)
            res = [g.to_group() for g in groups]
            log.debug(f"Retrieved manuscript groups: {len(res)}")
            return res

    def get_ppl_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.PersonGroup)
            groups = session.exec(statement)
            res = [g.to_group() for g in groups]
            log.debug(f"Retrieved people groups: {len(res)}")
            return res

    def get_txt_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.TextGroup)
            groups = session.exec(statement)
            res = [g.to_group() for g in groups]
            log.debug(f"Retrieved text groups: {len(res)}")
            return res

    def get_all_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups)
            groups = session.exec(statement)
            res = [g.to_group() for g in groups]
            log.debug(f"Retrieved groups: {len(res)}")
            return res

    def add_group(self, group: Group) -> None:
        with Session(self.engine) as session:
            db_model = Groups.make(group)
            session.add(db_model)
            session.commit()
            log.debug(f"Added group: {group.group_id}")

    def update_group(self, group: Group, group_id: UUID) -> None:
        with Session(self.engine) as session:
            group_old = session.get(Groups, group_id)
            if group_old is not None:
                session.delete(group_old)
            group_new = Groups.make(group)
            session.add(group_new)
            session.commit()
            log.debug(f"Updated group: {group_id}")

    def add_data(self, people: list[Person], catalogue_entries: list[CatalogueEntry], manuscripts: list[Manuscript]) -> None:
        log.info("Adding data to database...")
        self._add_people(people)
        log.info(f"People data added: {len(people)}")
        texts = list({t for ms in manuscripts for t in ms.texts})
        self._add_texts(texts)
        log.info(f"Text data added: {len(texts)}")
        self._add_catalogue_entries(catalogue_entries)
        log.info(f"Catalogue entries added: {len(catalogue_entries)}")
        self._add_manuscripts(manuscripts)
        log.info(f"Manuscripts added: {len(manuscripts)}")
        self._create_junction_tables(catalogue_entries, manuscripts)
        log.info("Junction tables created.")

    def _add_people(self, people: list[Person]) -> None:
        ppl = [People.make(p) for p in people]
        with Session(self.engine) as session:
            session.add_all(ppl)
            session.commit()

    def _add_texts(self, texts: list[str]) -> None:
        txt = [Texts(text_id=t) for t in texts]
        with Session(self.engine) as session:
            session.add_all(txt)
            session.commit()

    def _add_catalogue_entries(self, catalogue_entries: list[CatalogueEntry]) -> None:
        entries = [CatalogueEntries.make(e) for e in catalogue_entries]
        with Session(self.engine) as session:
            session.add_all(entries)
            session.commit()

    def _add_manuscripts(self, manuscripts: list[Manuscript]) -> None:
        mss = [Manuscripts.make(ms) for ms in manuscripts]
        with Session(self.engine) as session:
            session.add_all(mss)
            session.commit()

    def _create_junction_tables(self, catalogue_entries: list[CatalogueEntry], manuscripts: list[Manuscript]) -> None:
        txc = [TextCatalogueJunction(text_id=t, catalogue_id=c.catalogue_id) for c in catalogue_entries for t in c.texts]
        with Session(self.engine) as session:
            session.add_all(txc)
            session.commit()
        pxc = [PersonCatalogueJunction(pers_id=p, catalogue_id=c.catalogue_id) for c in catalogue_entries for p in c.people]
        with Session(self.engine) as session:
            session.add_all(pxc)
            session.commit()
        txm = [TextManuscriptJunction(text_id=t, manuscript_id=m.manuscript_id) for m in manuscripts for t in m.texts]
        with Session(self.engine) as session:
            session.add_all(txm)
            session.commit()
        pxm = [PersonManuscriptJunction(pers_id=p, manuscript_id=m.manuscript_id) for m in manuscripts for p in m.people]
        with Session(self.engine) as session:
            session.add_all(pxm)
            session.commit()
