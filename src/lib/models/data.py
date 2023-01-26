from sqlmodel import Field, SQLModel, Relationship
from typing import Optional


class PersonCatalogueJunction(SQLModel, table=True):
    pers_id: Optional[str] = Field(default=None, foreign_key="peopledbmodel.pers_id", primary_key=True)
    catalogue_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscriptcatalogueentrydbmodel.catalogue_id",
        primary_key=True
    )


class PersonManuscriptJunction(SQLModel, table=True):
    pers_id: Optional[str] = Field(default=None, foreign_key="peopledbmodel.pers_id", primary_key=True)
    manuscript_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscriptdbmodel.manuscript_id",
        primary_key=True
    )


class TextCatalogueJunction(SQLModel, table=True):
    text_id: Optional[str] = Field(default=None, foreign_key="textdbmodel.text_id", primary_key=True)
    catalogue_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscriptcatalogueentrydbmodel.catalogue_id",
        primary_key=True
    )


class TextManuscriptJunction(SQLModel, table=True):
    text_id: Optional[str] = Field(default=None, foreign_key="textdbmodel.text_id", primary_key=True)
    catalogue_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscriptdbmodel.manuscript_id",
        primary_key=True
    )


class TextDBModel(SQLModel, table=True):
    text_id: str = Field(primary_key=True)
    catalogue_entries: list["ManuscriptCatalogueEntryDBModel"] = Relationship(back_populates="texts", link_model=TextCatalogueJunction)
    manuscripts: list["ManuscriptDBModel"] = Relationship(back_populates="texts", link_model=TextManuscriptJunction)


class PeopleDBModel(SQLModel, table=True):
    pers_id: str = Field(primary_key=True)
    first_name: str
    last_name: str
    catalogue_entries: list["ManuscriptCatalogueEntryDBModel"] = Relationship(back_populates="people", link_model=PersonCatalogueJunction)
    manuscripts: list["ManuscriptDBModel"] = Relationship(back_populates="people", link_model=PersonManuscriptJunction)


class ManuscriptCatalogueEntryDBModel(SQLModel, table=True):
    catalogue_id: str = Field(primary_key=True)
    shelfmark: str
    manuscript_id: str
    catalogue_filename: str
    title: str
    description: str
    date_string: str
    terminus_post_quem: int
    terminus_ante_quem: int
    date_mean: int
    dating_range: int
    support: str
    folio: int
    height: str
    width: str
    extent: str
    origin: str
    creator: str
    country: str
    settlement: str
    repository: str
    texts: list[TextDBModel] = Relationship(back_populates="catalogue_entries", link_model=TextCatalogueJunction)
    people: list[PeopleDBModel] = Relationship(back_populates="catalogue_entries", link_model=PersonCatalogueJunction)


class ManuscriptDBModel(SQLModel, table=True):
    manuscript_id: str = Field(primary_key=True)
    shelfmark: str
    catalogue_entries: int
    catalogue_ids: str
    catalogue_filenames: str
    title: str
    description: str
    date_string: str
    terminus_post_quem: int
    termini_post_quos: str
    terminus_ante_quem: int
    termini_ante_quos: str
    date_standard_deviation: float
    support: str
    folio: int
    height: str
    width: str
    extent: str
    origin: str
    creator: str
    country: str
    settlement: str
    repository: str
    texts: list[TextDBModel] = Relationship(back_populates="manuscripts", link_model=TextManuscriptJunction)
    people: list[PeopleDBModel] = Relationship(back_populates="manuscripts", link_model=PersonManuscriptJunction)
