from __future__ import annotations

import statistics
from dataclasses import dataclass

from src.lib.xml.tamer import MetadataRowType


@dataclass(frozen=True)
class UnifiedMetadata:
    shelfmark: str
    catalogue_entries: int
    ms_title: str
    country: str
    settlement: str
    repository: str
    origin: str
    date_string: str
    terminus_post_quem_all: list[int]
    terminus_ante_quem_all: list[int]
    support: str
    folio: int
    height: str
    width: str
    extent: str
    description: str
    creator: str
    old_primary_key: str
    full_id: str
    filename: str

    @property
    def terminus_post_quem_mean(self) -> int:
        return int(statistics.mean(self.terminus_post_quem_all))

    @property
    def terminus_ante_quem_mean(self) -> int:
        return int(statistics.mean(self.terminus_ante_quem_all))

    @property
    def date_mean(self) -> int:
        return int(statistics.mean(self.terminus_ante_quem_all + self.terminus_post_quem_all))

    @property
    def date_sd(self) -> float:
        return statistics.stdev(self.terminus_ante_quem_all + self.terminus_post_quem_all)

    @staticmethod
    def from_entry(e: MetadataRowType) -> UnifiedMetadata:
        return UnifiedMetadata(
            shelfmark=e[0],
            catalogue_entries=1,
            ms_title=e[1],
            country=e[2],
            settlement=e[3],
            repository=e[4],
            origin=e[5],
            date_string=e[6],
            terminus_post_quem_all=[e[7]],
            terminus_ante_quem_all=[e[8]],
            support=e[11],
            folio=e[12],
            height=e[13],
            width=e[14],
            extent=e[15],
            description=e[16],
            creator=e[17],
            old_primary_key=e[18],
            full_id=e[19],
            filename=e[20]
        )

    def to_tuple(self) -> tuple[
        str,  # shelfmark
        int,  # catalogue_entries
        str,  # ms_title
        str,  # country
        str,  # settlement
        str,  # repository
        str,  # origin
        str,  # date_string
        int,  # terminus_post_quem_mean # TODO: should this be here? as it's a property not a field
        str,  # terminus_post_quem_all
        int,  # terminus_ante_mean_all # TODO: dito
        str,  # terminus_ante_quem_all
        int,  # date mean
        float,  # date standard deviation
        str,  # support
        int,  # folio
        str,  # height
        str,  # width
        str,  # extent
        str,  # description
        str,  # creator
        str,  # old_primary_key
        str,  # full_id
        str  # filename
    ]:
        return (
            self.shelfmark,
            self.catalogue_entries,
            self.ms_title,
            self.country,
            self.settlement,
            self.repository,
            self.origin,
            self.date_string,
            self.terminus_post_quem_mean,
            '|'.join((str(d) for d in self.terminus_post_quem_all)),
            self.terminus_ante_quem_mean,
            '|'.join((str(d) for d in self.terminus_ante_quem_all)),
            self.date_mean,
            self.date_sd,
            self.support,
            self.folio,
            self.height,
            self.width,
            self.extent,
            self.description,
            self.creator,
            self.old_primary_key,
            self.full_id,
            self.filename
        )

    def __add__(self, other: UnifiedMetadata) -> UnifiedMetadata:
        if self.shelfmark != other.shelfmark:
            print(f"WARNING: Combining manuscripts with different shelfmarks: {self.shelfmark} != {other.shelfmark}")
            with open('zzz_diff_shelfmarks.txt', 'a', encoding='utf-8') as f:
                f.write(f"{self.shelfmark}\t{other.shelfmark}\n")
        if self.folio != other.folio:
            print(f"WARNING: Folio number mismatch: {self.folio} != {other.folio} in manuscript: {self.shelfmark}")
            with open('zzz_diff_fol.txt', 'a', encoding='utf-8') as f2:
                f2.write(f"{self.folio} != {other.folio} in manuscript: {self.shelfmark}\n")
        tps = list(set(self.terminus_post_quem_all + other.terminus_post_quem_all) - {0}) or [0]
        tas = list(set(self.terminus_ante_quem_all + other.terminus_ante_quem_all) - {0}) or [0]
        return UnifiedMetadata(
            shelfmark=combine_str(self.shelfmark, other.shelfmark),
            catalogue_entries=self.catalogue_entries + other.catalogue_entries,
            ms_title=combine_str(self.ms_title, other.ms_title),
            country=combine_str(self.country, other.country),
            settlement=combine_str(self.settlement, other.settlement),
            repository=combine_str(self.repository, other.repository),
            origin=combine_str(self.origin, other.origin),
            date_string=combine_str(self.date_string, other.date_string),
            terminus_post_quem_all=tps,
            terminus_ante_quem_all=tas,
            support=combine_str(self.support, other.support),
            folio=combine_int(self.folio, other.folio),
            height=combine_str(self.height, other.height),
            width=combine_str(self.width, other.width),
            extent=combine_str(self.extent, other.extent),
            description=combine_str(self.description, other.description),
            creator=combine_str(self.creator, other.creator),
            old_primary_key=f"{self.old_primary_key}|{other.old_primary_key}",
            full_id=f"{self.full_id}|{other.full_id}",
            filename=f"{self.filename}|{other.filename}",
        )


def unify_metadata_entries(entries: list[MetadataRowType]) -> UnifiedMetadata:
    n = len(entries)
    if n > 3:
        print(f"WARNING: Trouble deduplicating '{n}' entries for {entries[0][0]}")
        with open('zzz_4plus.txt', 'a', encoding='utf-8') as f:
            issue = '\n'.join((f'    {x}' for x in entries))
            f.write(f"{n} entries for {entries[0][0]}:\n{issue}\n")
    u_entries = [UnifiedMetadata.from_entry(e) for e in entries]
    res = u_entries.pop()
    for u in u_entries:
        res += u
    return res


def get_unified_metadata(ms_metadata: list[MetadataRowType]) -> list[UnifiedMetadata]:
    metadata_per_id: dict[str, list[MetadataRowType]] = {}
    for m in ms_metadata:
        id_ = m[18]
        c = metadata_per_id.get(id_, [])
        metadata_per_id[id_] = c + [m]
    return [unify_metadata_entries(m) for m in metadata_per_id.values()]


def combine_str(x: str, y: str) -> str:
    disregard = {"origin unknown", "n/a"}
    dk = {"Danmark", "Denmark"}
    cph = {"København", "Copenhagen"}
    if x == y:
        return x
    if y in x:
        return x
    if x in y:
        return y
    if x.lower() in disregard:
        return y
    if y.lower() in disregard:
        return x
    if {x, y} == dk:
        return "Denmark"
    if {x, y} == cph:
        return "Copenhagen"
    r = ' | '.join((x, y))
    print(r)
    return r


def combine_int(x: int, y: int) -> int:
    if x == y:
        return x
    if x == 0:
        return y
    if y == 0:
        return x
    print(f"{x} != {y}")
    return int((x + y) / 2)
