from src.lib.xml.tamer import MetadataRowType


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


def get_unified_metadata(ms_metadata: list[MetadataRowType]) -> list[MetadataRowType]:

    d: dict[str, list[MetadataRowType]] = {}
    for m in ms_metadata:
        id_ = m[18]
        c = d.get(id_, [])
        d[id_] = c + [m]

    res = []
    for v in d.values():
        n = len(v)
        if n == 1:
            res.append(v[0])
        elif n == 2:
            m1 = v[0]
            m2 = v[1]
            if m1[0] != m2[0]:
                print(f"WARNING: Shelfmarks {m1[0]} and {m2[0]} are not equal!")
            r: MetadataRowType = (
                m1[0],  # shelfmark
                combine_str(m1[1], m2[1]),  # shorttitle
                combine_str(m1[2], m2[2]),  # country
                combine_str(m1[3], m2[3]),  # settlement
                combine_str(m1[4], m2[4]),  # repository
                combine_str(m1[5], m2[5]),  # origin
                combine_str(m1[6], m2[6]),  # date
                combine_int(m1[7], m2[7]),  # tp
                combine_int(m1[8], m2[8]),  # ta
                0,
                0,
                "...",
                0,
                "...",
                "...",
                "...",
                "...",
                "...",
                "...",
                "...",
                "...",
            )
            res.append(r)
        elif n == 3:
            pass  # TODO: combine 3
        else:
            # TODO: ???
            pass
            # print("Nope!")
            # print(n)
            # for vv in v:
            #     print(vv)
            # raise ValueError("Did not expect that.")
    return res
