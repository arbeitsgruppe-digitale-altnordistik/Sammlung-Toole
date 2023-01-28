from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CatalogueEntry:  # TODO-BL: make some optional?
    catalogue_id: str
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
    texts: list[str]
    people: list[str]


@dataclass(frozen=True)
class Manuscript:
    manuscript_id: str
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
    texts: list[str]
    people: list[str]
