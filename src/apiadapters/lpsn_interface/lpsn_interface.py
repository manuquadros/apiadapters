import pathlib
import string
import tomllib
from functools import lru_cache
from numbers import Integral
from typing import cast

import pandas as pd
from cacheout import Cache
from loggers import logger, stderr_logger

with open(file=pathlib.Path(__file__).parent / "config.toml", mode="rb") as cf:
    config = tomllib.load(cf)

cache = Cache()

file_logger = logger(filename="lpsn.log")


@cache.memoize()
def get_lpsn() -> pd.DataFrame:
    """Read (local) LPSN data.

    :returns: Pandas DataFrame with the LPSN data.
    """
    df = pd.read_csv(config["lpsn"])
    df = df.drop(
        ["reference", "authors", "risk_grp", "nomenclatural_type"],
        axis="columns",
    )
    df = df.fillna("")

    return df


def lpsn_name(record: pd.Series) -> str:
    """Assemble a species name from the fields of a record in the LPSN data."""
    subs_epithet = record["subsp_epithet"]

    if subs_epithet:
        subs_epithet = "subsp. " + subs_epithet

    return " ".join(
        (record["genus_name"], record["sp_epithet"], subs_epithet)
    ).strip()


@lru_cache
def lpsn_synonyms(query: int | str) -> frozenset[str]:
    """Collect the synonyms of a given species name from the LPSN data.

    :param query: record number of the species designation in the LPSN data

    :returns: set of synonyms for the species in record no. `query`.
    """
    if isinstance(query, Integral):
        lpsn = get_lpsn()
        own_lnk = lpsn.query("record_no == @query")["record_lnk"].values[0]
        syn_records = lpsn.query("record_lnk == @query | record_no == @own_lnk")

        if syn_records.empty:
            return frozenset()

        names = syn_records.apply(lpsn_name, axis=1)
        return frozenset(names)

    if isinstance(query, str):
        _id = lpsn_id(cast(str, query))
        return lpsn_synonyms(_id) if _id else frozenset()

    stderr_logger().error(
        "Invalid LPSN synonym query: %s is not int or string.",
        query,
    )
    return frozenset()


@lru_cache
def name_parts(name: str) -> dict[str, str]:
    """Collect the relevant components of a species name for an LPSN query.

    :param name: name of the species
    :returns: dict keyed by "genus_name", "sp_epithet", "subsp_epithet", and "strain",
              containing the respective components of `name`.
    """
    name_parts = (
        name.replace("subsp.", "")
        .replace("ssp.", "")
        .replace("sp.", "")
        .replace("pv.", "")
        .replace("serovar", "")
        .replace("serotype", "")
        .split()
    )
    keys = ("genus_name", "sp_epithet", "subsp_epithet", "strain")
    out = {key: "" for key in keys}

    for index, term in enumerate(name_parts[:4]):
        if any(char not in string.ascii_lowercase for char in term[1:]):
            out["strain"] = term
            break
        out[keys[index]] = term

    return out


@lru_cache
def lpsn_id(name: str) -> int | None:
    """Retrieve the record number of `name` in LPSN, if it exists."""
    lpsn = get_lpsn()
    keys = ("genus_name", "sp_epithet", "subsp_epithet")
    parts = name_parts(name)

    query = " & ".join(f"{key} == '{parts[key]}'" for key in keys)

    try:
        record = lpsn.query(query).iloc[0]
    except IndexError:
        file_logger.error(
            ("Couldn't find an LPSN record for %s. The query was %s."),
            name,
            query,
        )
        return None

    return int(record["record_no"])


@lru_cache
def lpsn_parent(_id: int) -> tuple[int, str] | None:
    """Retrieve the record number and name to which `_id` is linked, if any."""
    lpsn = get_lpsn()

    try:
        parent_id = lpsn[lpsn["record_no"] == _id]["record_lnk"].iloc[0]
        record = lpsn[lpsn["record_no"] == parent_id].iloc[0]
        return (record["record_no"], lpsn_name(record))
    except IndexError:
        return None
