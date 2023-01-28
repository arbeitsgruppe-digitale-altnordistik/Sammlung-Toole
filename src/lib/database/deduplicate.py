from __future__ import annotations

import statistics

from src.lib.manuscripts import CatalogueEntry, Manuscript


def get_unified_metadata(entries: list[CatalogueEntry]) -> list[Manuscript]:
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
    entries_per_ms: dict[str, list[CatalogueEntry]] = {}
    for e in entries:
        k = e.manuscript_id
        g = entries_per_ms.get(k, [])
        entries_per_ms[k] = g + [e]
    return [_unify_metadata_entries(m) for m in entries_per_ms.values()]


def _unify_metadata_entries(entries: list[CatalogueEntry]) -> Manuscript:
    """Combines a list of ununified manuscript metadata *with the same ID* into one unified entry.

    Args:
        entries (list[MetadataRowType]): a list of manuscript metadata, where all entries have the same manuscript ID

    Returns:
        UnifiedMetadata: A single manuscript entry with unified metadata in it
    """
    n = len(entries)
    if n > 3:
        ms_id = entries[0].manuscript_id
        print(f"WARNING: Trouble deduplicating '{n}' entries for {ms_id}")
        with open('zzz_4plus.txt', 'a', encoding='utf-8') as f:
            issue = '\n'.join((f'    {x}' for x in entries))
            f.write(f"{n} entries for {ms_id}:\n{issue}\n")
    tps = [e.terminus_post_quem for e in entries]
    tas = [e.terminus_ante_quem for e in entries]
    return Manuscript(
        manuscript_id=combine_strs(*[e.manuscript_id for e in entries]),
        shelfmark=combine_strs(*[e.shelfmark for e in entries]),
        catalogue_entries=len(entries),
        catalogue_ids=" | ".join(e.catalogue_id for e in entries),
        catalogue_filenames=" | ".join(e.catalogue_filename for e in entries),
        title=combine_strs(*[e.title for e in entries]),
        description=combine_strs(*[e.description for e in entries]),
        date_string=combine_strs(*[e.date_string for e in entries]),
        terminus_post_quem=max(tps),
        termini_post_quos=combine_strs(*[str(t) for t in tps]),
        terminus_ante_quem=min(tas),
        termini_ante_quos=combine_strs(*[str(t) for t in tas]),
        date_mean=int(statistics.mean(tps + tas)),
        date_standard_deviation=statistics.stdev(tps + tas),
        support=combine_strs(*[e.support for e in entries]),
        folio=combine_ints(*[e.folio for e in entries]),
        height=combine_strs(*[e.height for e in entries]),
        width=combine_strs(*[e.width for e in entries]),
        extent=combine_strs(*[e.extent for e in entries]),
        origin=combine_strs(*[e.origin for e in entries]),
        creator=combine_strs(*[e.creator for e in entries]),
        country=combine_strs(*[e.country for e in entries]),
        settlement=combine_strs(*[e.settlement for e in entries]),
        repository=combine_strs(*[e.repository for e in entries]),
        texts=list(set.union(*[set(e.texts) for e in entries])),
        people=list(set.union(*[set(e.people) for e in entries]))
    )


def combine_strs(s: str, *ss: str) -> str:
    if len(ss) < 1:
        return s
    elif len(ss) == 1:
        return combine_str(s, ss[0])
    else:
        return combine_str(s, combine_strs(*ss))


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
    disregard = {"origin unknown", "n/a", "null"}
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
    print(f"Failed to unify: {r}")
    return r


def combine_ints(x: int, *xx: int) -> int:
    if len(xx) < 1:
        return x
    elif len(xx) == 1:
        return combine_int(x, xx[0])
    else:
        return combine_int(x, combine_ints(*xx))


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
    print(f"Failed to unify: {x} != {y}")
    return int((x + y) / 2)
