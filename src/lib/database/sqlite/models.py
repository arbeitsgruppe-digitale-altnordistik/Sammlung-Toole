from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, Relationship, SQLModel

from src.lib.groups import Group, GroupType
from src.lib.manuscripts import CatalogueEntry, Manuscript
from src.lib.people import Person


class Groups(SQLModel, table=True):
    group_id: UUID = Field(primary_key=True, default_factory=uuid4)
    group_type: GroupType
    name: str
    date: str
    items: str

    def to_group(self) -> Group:
        data = self.dict()
        data["date"] = datetime.fromtimestamp(float(self.date), timezone.utc).astimezone()
        data["items"] = set(self.items.split("|")) if data["items"] else set()
        return Group(**data)

    @staticmethod
    def make(group: Group) -> Groups:
        data = {**group.__dict__}
        data["date"] = group.date.timestamp()
        data["items"] = '|'.join(group.items)
        return Groups(**data)


class PersonCatalogueJunction(SQLModel, table=True):
    pers_id: Optional[str] = Field(default=None, foreign_key="people.pers_id", primary_key=True)
    catalogue_id: Optional[str] = Field(
        default=None,
        foreign_key="catalogueentries.catalogue_id",
        primary_key=True
    )


class PersonManuscriptJunction(SQLModel, table=True):
    pers_id: Optional[str] = Field(default=None, foreign_key="people.pers_id", primary_key=True)
    manuscript_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscripts.manuscript_id",
        primary_key=True
    )


class TextCatalogueJunction(SQLModel, table=True):
    text_id: Optional[str] = Field(default=None, foreign_key="texts.text_id", primary_key=True)
    catalogue_id: Optional[str] = Field(
        default=None,
        foreign_key="catalogueentries.catalogue_id",
        primary_key=True
    )


class TextManuscriptJunction(SQLModel, table=True):
    text_id: Optional[str] = Field(default=None, foreign_key="texts.text_id", primary_key=True)
    manuscript_id: Optional[str] = Field(
        default=None,
        foreign_key="manuscripts.manuscript_id",
        primary_key=True
    )


class Texts(SQLModel, table=True):
    text_id: str = Field(primary_key=True)
    catalogue_entries: list["CatalogueEntries"] = Relationship(back_populates="texts", link_model=TextCatalogueJunction)
    manuscripts: list["Manuscripts"] = Relationship(back_populates="texts", link_model=TextManuscriptJunction)


class People(SQLModel, table=True):
    pers_id: str = Field(primary_key=True)
    first_name: str | None = None
    last_name: str | None = None
    catalogue_entries: list["CatalogueEntries"] = Relationship(back_populates="people", link_model=PersonCatalogueJunction)
    manuscripts: list["Manuscripts"] = Relationship(back_populates="people", link_model=PersonManuscriptJunction)

    @staticmethod
    def make(person: Person) -> People:
        return People(
            pers_id=person.pers_id,
            first_name=person.first_name,
            last_name=person.last_name
        )


class CatalogueEntries(SQLModel, table=True):
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
    texts: list[Texts] = Relationship(back_populates="catalogue_entries", link_model=TextCatalogueJunction)
    people: list[People] = Relationship(back_populates="catalogue_entries", link_model=PersonCatalogueJunction)

    @staticmethod
    def make(entry: CatalogueEntry) -> CatalogueEntries:
        data = {**entry.__dict__}
        data["texts"] = []
        data["people"] = []
        return CatalogueEntries(**data)


class Manuscripts(SQLModel, table=True):
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
    date_mean: int
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
    texts: list[Texts] = Relationship(back_populates="manuscripts", link_model=TextManuscriptJunction)
    people: list[People] = Relationship(back_populates="manuscripts", link_model=PersonManuscriptJunction)

    @staticmethod
    def make(entry: Manuscript) -> Manuscripts:
        data = {**entry.__dict__}
        data["texts"] = []
        data["people"] = []
        return Manuscripts(**data)
