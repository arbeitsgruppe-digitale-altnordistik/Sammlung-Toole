from __future__ import annotations

import statistics
from dataclasses import dataclass

from src.lib.xml.tamer import MetadataRowType


@dataclass(frozen=True)
class UnifiedMetadata:
    """
    Model/Dataclass for a single entry of unified manuscript meta data

    Args:
        shelfmark (str): the manuscript shelfmark
        catalogue_entries (int): the number of entries that were combined into this unified entry

    """
    # CHORE: documentation
    handrit_id: str
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
    full_id: str
    filename: str

    @property
    def terminus_post_quem(self) -> int:
        return max(self.terminus_post_quem_all)

    @property
    def terminus_post_quem_mean(self) -> int:
        """The statistical mean of the termini post quos"""
        return int(statistics.mean(self.terminus_post_quem_all))

    @property
    def terminus_ante_quem(self) -> int:
        return min(self.terminus_ante_quem_all)

    @property
    def terminus_ante_quem_mean(self) -> int:
        """The statistical mean of the termini ante quos"""
        return int(statistics.mean(self.terminus_ante_quem_all))

    @property
    def date_mean(self) -> int:
        """The statistical mean of the ,amuscript datings"""
        return int(statistics.mean(self.terminus_ante_quem_all + self.terminus_post_quem_all))

    @property
    def date_sd(self) -> float:
        """The statistical standard deviation of the termini post et ante quos"""
        return statistics.stdev(self.terminus_ante_quem_all + self.terminus_post_quem_all)

    @staticmethod
    def from_entry(e: MetadataRowType) -> UnifiedMetadata:
        """Creates an object of type UNifiedMetadata from a single row from the manuscripts database"""
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
            handrit_id=e[18],
            full_id=e[19],
            filename=e[20]
        )

    def to_tuple(self) -> tuple[
        str,  # handrit_id
        str,  # shelfmark
        int,  # catalogue_entries
        str,  # ms_title
        str,  # country
        str,  # settlement
        str,  # repository
        str,  # origin
        str,  # date_string
        int,  # terminus post quem
        str,  # terminus_post_quem_all
        int,  # terminus ante quem
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
        str,  # full_id
        str  # filename
    ]:
        """Returns the data of this object as a tuple for stroing in the database"""
        return (
            self.handrit_id,
            self.shelfmark,
            self.catalogue_entries,
            self.ms_title,
            self.country,
            self.settlement,
            self.repository,
            self.origin,
            self.date_string,
            self.terminus_post_quem,
            '|'.join((str(d) for d in self.terminus_post_quem_all)),
            self.terminus_ante_quem,
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
            self.full_id,
            self.filename
        )

    def __add__(self, other: UnifiedMetadata) -> UnifiedMetadata:
        """
        Combines the UnifiedMetadata object with an other UnifiedMetadata object.

        Operator overload so that we can use `+` on these objects
        """
        if self.shelfmark != other.shelfmark:
            print(f"WARNING: Combining manuscripts with different shelfmarks: {self.shelfmark} != {other.shelfmark}")
            with open('zzz_diff_shelfmarks.txt', 'a', encoding='utf-8') as f:
                f.write(f"{self.shelfmark}\t{other.shelfmark}\n")
        if self.handrit_id != other.handrit_id:
            print(f"WARNING: Combining manuscripts with different IDs: {self.handrit_id} != {other.handrit_id}")
            with open('zzz_diff_ids.txt', 'a', encoding='utf-8') as f:
                f.write(f"{self.handrit_id}\t{other.handrit_id}\n")
        if self.folio != other.folio:
            print(f"WARNING: Folio number mismatch: {self.folio} != {other.folio} in manuscript: {self.shelfmark}")
            with open('zzz_diff_fol.txt', 'a', encoding='utf-8') as f2:
                f2.write(f"{self.folio} != {other.folio} in manuscript: {self.shelfmark}\n")
        tps = list(set(self.terminus_post_quem_all + other.terminus_post_quem_all) - {0}) or [0]
        tas = list(set(self.terminus_ante_quem_all + other.terminus_ante_quem_all) - {0}) or [0]
        return UnifiedMetadata(
            handrit_id=self.handrit_id,
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
            full_id=f"{self.full_id}|{other.full_id}",
            filename=f"{self.filename}|{other.filename}",
        )


def _unify_metadata_entries(entries: list[MetadataRowType]) -> UnifiedMetadata:
    """Combines a list of ununified manuscript metadata *with the same ID* into one unified entry.

    Args:
        entries (list[MetadataRowType]): a list of manuscript metadata, where all entries have the same manuscript ID

    Returns:
        UnifiedMetadata: A single manuscript entry with unified metadata in it
    """
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
    """Combine all metadata with of the same ID into single entries.

    Given a list of rows from the db table of non-unified manuscript metadata,
    this method returns a list of unified manuscript metadata, where all entries
    that concern the same maniuscript ID are merged into one entry.

    Note that this process is potentially "lossy", i.e. in some edge cases,
    the result may not be as sensible as the input.

    The result also contains some information on the differences of input.

    Args:
        ms_metadata (list[MetadataRowType]): data as contained by the manuscripts table in the DB

    Returns:
        list[UnifiedMetadata]: unified data as modelled in `UnifiedMetadata`
    """
    metadata_per_id: dict[str, list[MetadataRowType]] = {}
    for m in ms_metadata:
        id_ = m[18]
        c = metadata_per_id.get(id_, [])
        metadata_per_id[id_] = c + [m]
    return [_unify_metadata_entries(m) for m in metadata_per_id.values()]


def combine_str(x: str, y: str) -> str:
    """
    Combine two strings into a "unified" string.

    Works under the following assumptions:
        - if the two strings are equal, this value is returned
        - if one of the strings contains the other, then the containing string is returned
        - if one of the strings is contained by a given set of values to disregard ("unknown", "N/A", etc.)
          then the other string is returned, even though the other string might still be in this set too.
          (That means that "N/A" + "Origin Unknown" returns "Origin Unknown")
        - if the two strings make up a well-known, given pair of strings (e.g. "København", "Copenhagen"),
          then one of the values (here "Copenhagen") is returned.
        - if none of these conditions are met, then the two strings are combined with the ` | ` seperator
    """
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
    """
    Combines two integers.

    Assumptions:
        - if the values are identical, return this value
        - if one value is 0, return the other one (even though that may be 0 too)
        - otherwise, return the mean of the two
    """
    if x == y:
        return x
    if x == 0:
        return y
    if y == 0:
        return x
    print(f"{x} != {y}")
    return int((x + y) / 2)
