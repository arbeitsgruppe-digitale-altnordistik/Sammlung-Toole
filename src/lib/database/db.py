from dataclasses import dataclass, field
from typing import Protocol
import pandas as pd
from sqlalchemy.future import Engine
# from sqlalchemy.orm import joinedload
from src.lib.constants import DATABASE_PATH_TMP
from sqlmodel import Session, SQLModel, create_engine, select, col
from src.lib.models.data import *


class MainDB(Protocol):
    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        """One stop shop for simple search/SELECT queries.

        Args:
            table_name(str): Name of table to be queried
            column_name(str): Name of column on which to apply selection criteria
            search_criteria(List): What it is you are looking for

        Returns:
            pd.DataFrame
        """
        ...

    # TODO-BL: left out `simple_people_search()` here... check if still needed

    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        """Get IDs of all manuscripts related to a list of people."""
        ...

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        """
        Get IDs of all people connected to a list of manuscripts.
        Returns list of IDs for people.
        """
        ...

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        """
        Get IDs of all manuscripts connected to a list of texts.
        Returns list of IDs for manuscripts.
        """
        # TODO: clarify text definition
        ...

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        """
        Get IDs of all texts connected to a list of manuscripts.
        Returns list of IDs for texts.
        """
        # TODO: clarify text definition
        ...

    def persons_lookup_dict(self) -> dict[str, str]:
        """
        Gets the data from person(ID, first name, last name).
        Returns the lookup-dict for the IDs of people to their natural names.
        """
        ...

    def ms_lookup_dict(self) -> dict[str, list[str]]:
        """Returns the lookup-dict for the IDs of manuscripts to their human readable signatures."""
        ...

    def txt_lookup_list(self) -> list[str]:
        """Returns the lookup-list for the texts. Used in front end search."""
        ...


def get_engine(db_path: str = DATABASE_PATH_TMP) -> Engine:
    sqlite_url = f"sqlite:///{db_path}"
    return create_engine(sqlite_url)


@dataclass(frozen=True)
class MainDBImpl:
    engine: Engine = field(default_factory=get_engine, init=False)

    def setup_db(self) -> None:
        SQLModel.metadata.create_all(self.engine)

    def get_metadata(self, ms_ids: list[str]) -> pd.DataFrame:
        with Session(self.engine) as session:
            statement = select(ManuscriptDBModel).where(col(ManuscriptDBModel.manuscript_id).in_(ms_ids))
            mss = session.exec(statement).all()
            msdicts = [ms.dict() for ms in mss]
            return pd.DataFrame(msdicts)

    # TODO: may be possible to improve performance with some kind of eager querying (goes for all those here)
    def ms_x_ppl(self, pers_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(
                ManuscriptDBModel.manuscript_id
            ).where(
                col(ManuscriptDBModel.people).any(col(PeopleDBModel.pers_id).in_(pers_ids))
            )
            mss = session.exec(statement).all()
            return mss

    def ppl_x_mss(self, ms_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(
                PeopleDBModel.pers_id
            ).where(
                col(PeopleDBModel.manuscripts).any(col(ManuscriptDBModel.manuscript_id).in_(ms_ids))
            )
            mss = session.exec(statement).all()
            return mss

    def ms_x_txts(self, txts: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(
                ManuscriptDBModel.manuscript_id
            ).where(
                col(ManuscriptDBModel.texts).any(col(TextDBModel.text_id).in_(txts))
            )
            mss = session.exec(statement).all()
            return mss

    def txts_x_ms(self, ms_ids: list[str]) -> list[str]:
        with Session(self.engine) as session:
            statement = select(
                TextDBModel.text_id
            ).where(
                col(TextDBModel.manuscripts).any(col(ManuscriptDBModel.manuscript_id).in_(ms_ids))
            )
            mss = session.exec(statement).all()
            return mss

    def persons_lookup_dict(self) -> dict[str, str]:
        with Session(self.engine) as session:
            ppl = session.exec(select(PeopleDBModel)).all()
            res = {p.pers_id: f"{p.first_name} {p.last_name}" for p in ppl}
            return res

    def ms_lookup_dict(self) -> dict[str, list[str]]:
        with Session(self.engine) as session:
            statement = select(ManuscriptDBModel.manuscript_id, ManuscriptDBModel.shelfmark, ManuscriptDBModel.title)
            mss = session.exec(statement).all()
            res = {x[0]: [x[1], x[2]] for x in mss}
            return res

    def txt_lookup_list(self) -> list[str]:
        with Session(self.engine) as session:
            res = session.exec(select(TextDBModel.text_id)).all()
            return res

    # def test_populate(self) -> None:
    #     with Session(self.engine) as session:
    #         text1 = TextDBModel(text_id="text 1")
    #         text2 = TextDBModel(text_id="text 2")
    #         ms1 = ManuscriptDBModel(
    #             manuscript_id="manuscript 1",
    #             shelfmark="ms1",
    #             catalogue_entries=1,
    #             catalogue_ids="ms1_da",
    #             catalogue_filenames="ms1_da.xml",
    #             title="some manuscript",
    #             description="",
    #             date_string="",
    #             terminus_post_quem=1200,
    #             termini_post_quos="1200",
    #             terminus_ante_quem=1500,
    #             termini_ante_quos="1500",
    #             date_standard_deviation=0.0,
    #             support="parchment",
    #             folio=50,
    #             height="300",
    #             width="200",
    #             extent="50",
    #             origin="Iceland",
    #             creator="Unknown",
    #             country="Iceland",
    #             settlement="Reykjavik",
    #             repository="AM",
    #             texts=[text1, text2]
    #         )
    #         ms2 = ManuscriptDBModel(
    #             manuscript_id="manuscript 2",
    #             shelfmark="ms2",
    #             catalogue_entries=1,
    #             catalogue_ids="ms2_da",
    #             catalogue_filenames="ms2_da.xml",
    #             title="some other manuscript",
    #             description="",
    #             date_string="",
    #             terminus_post_quem=1200,
    #             termini_post_quos="1200",
    #             terminus_ante_quem=1500,
    #             termini_ante_quos="1500",
    #             date_standard_deviation=0.0,
    #             support="parchment",
    #             folio=50,
    #             height="300",
    #             width="200",
    #             extent="50",
    #             origin="Iceland",
    #             creator="Unknown",
    #             country="Iceland",
    #             settlement="Reykjavik",
    #             repository="AM",
    #             texts=[text1]
    #         )
    #         session.add(ms1)
    #         session.add(ms2)
    #         session.commit()
