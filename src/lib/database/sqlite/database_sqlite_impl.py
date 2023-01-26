from dataclasses import dataclass, field
from uuid import UUID

import pandas as pd
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel, col, create_engine, select

from src.lib.constants import DATABASE_PATH_TMP
from src.lib.database.sqlite.models import *
from src.lib.groups import Group, GroupType


def get_engine(db_path: str = DATABASE_PATH_TMP) -> Engine:
    sqlite_url = f"sqlite:///{db_path}"
    return create_engine(sqlite_url)


@dataclass(frozen=True)
class DatabaseSQLiteImpl:
    engine: Engine = field(default_factory=get_engine, init=False)

    def setup_db(self) -> None:
        SQLModel.metadata.create_all(self.engine)

    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        with Session(self.engine) as session:
            statement = select(Manuscripts).where(col(Manuscripts.manuscript_id).in_(ms_ids))
            mss = session.exec(statement).all()
            ms_dicts = [ms.dict() for ms in mss]
            return pd.DataFrame(ms_dicts)

    # TODO: may be possible to improve performance with some kind of eager querying (goes for all those here)
    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id).where(col(Manuscripts.people).any(col(People.pers_id).in_(pers_ids)))
            mss = session.exec(statement).all()
            return mss

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(People.pers_id).where(col(People.manuscripts).any(col(Manuscripts.manuscript_id).in_(ms_ids)))
            mss = session.exec(statement).all()
            return mss

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id).where(col(Manuscripts.texts).any(col(Texts.text_id).in_(txts)))
            mss = session.exec(statement).all()
            return mss

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(Texts.text_id).where(col(Texts.manuscripts).any(col(Manuscripts.manuscript_id).in_(ms_ids)))
            mss = session.exec(statement).all()
            return mss

    def persons_lookup_dict(self) -> dict[str, str]:
        with Session(self.engine) as session:
            ppl = session.exec(select(People)).all()
            res = {p.pers_id: f"{p.first_name} {p.last_name}" for p in ppl}
            return res

    def ms_lookup_dict(self) -> dict[str, list[str]]:
        with Session(self.engine) as session:
            statement = select(Manuscripts.manuscript_id, Manuscripts.shelfmark, Manuscripts.title)
            mss = session.exec(statement).all()
            res = {x[0]: [x[1], x[2]] for x in mss}
            return res

    def txt_lookup_list(self) -> list[str]:
        with Session(self.engine) as session:
            res = session.exec(select(Texts.text_id)).all()
            return res

    def get_ms_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.ManuscriptGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_ppl_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.PersonGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_txt_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups).where(Groups.group_type == GroupType.TextGroup)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def get_all_groups(self) -> list[Group]:
        with Session(self.engine) as session:
            statement = select(Groups)
            groups = session.exec(statement)
            return [g.to_group() for g in groups]

    def add_group(self, group: Group) -> None:
        with Session(self.engine) as session:
            db_model = Groups.make(group)
            session.add(db_model)
            session.commit()

    def update_group(self, group: Group, group_id: UUID) -> None:
        with Session(self.engine) as session:
            group_old = session.get(Groups, group_id)
            if group_old is not None:
                session.delete(group_old)
            group_new = Groups.make(group)
            session.add(group_new)
            session.commit()
