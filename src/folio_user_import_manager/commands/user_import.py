"""Command for importing user data into FOLIO."""

import typing
from dataclasses import dataclass

import httpx
import polars as pl
import polars.selectors as cs
from pyfolioclient import BadRequestError, UnprocessableContentError

from folio_user_import_manager.data import InputData, InputDataOptions
from folio_user_import_manager.folio import Folio, FolioOptions


@dataclass(frozen=True)
class ImportOptions(InputDataOptions, FolioOptions):
    """Options used for importing users into FOLIO."""

    batch_size: int
    max_concurrency: int
    retry_count: int
    failed_user_threshold: float

    deactivate_missing_users: bool
    update_all_fields: bool
    source_type: str | None


@dataclass
class ImportResults:
    """Results of importing users into FOLIO."""

    created_records: int = 0
    failed_records: int = 0


def _clean_nones(obj: dict[str, typing.Any]) -> dict[str, typing.Any]:
    for k in list(obj.keys()):
        if k in ["customFields", "requestPreference"]:
            _clean_nones(obj[k])
        if k == "personal":
            if "addresses" in obj[k]:
                for a in obj[k]["addresses"]:
                    _clean_nones(a)
                obj[k]["addresses"] = [a for a in obj[k]["addresses"] if a != {}]
            _clean_nones(obj["personal"])
        if obj[k] is None or obj[k] == {} or obj[k] == []:
            del obj[k]

    return obj


def _transform_batch(batch: pl.LazyFrame) -> pl.LazyFrame:
    cols = batch.collect_schema().names()
    for c in cols:
        if c in ["departments", "preferredEmailCommunication"]:
            batch = batch.with_columns(pl.col(c).str.split(","))
        if c in ["customFields"]:
            batch = batch.with_columns(pl.col(c).str.json_decode())
        if c in ["enrollmentDate", "expirationDate", "personal_dateOfBirth"]:
            batch = batch.with_columns(pl.col(c).dt.to_string())

    cs_primary = cs.starts_with("personal_address_primary_")
    cs_secondary = cs.starts_with("personal_address_secondary_")
    cs_addresses = cs.starts_with("personal_address_")
    cs_personal = cs.starts_with("personal_") - cs_addresses
    cs_req_pref = cs.starts_with("requestPreference_")

    primary_names = [
        c.replace("personal_address_primary_", "")
        for c in cols
        if c.startswith("personal_address_primary_")
    ]
    if any(primary_names):
        batch = batch.with_columns(
            pl.struct(cs_primary)
            .struct.rename_fields(primary_names)
            .alias("personal_address_primary"),
        )
    secondary_names = [
        c.replace("personal_address_secondary_", "")
        for c in cols
        if c.startswith("personal_address_secondary_")
    ]
    if any(secondary_names):
        batch = batch.with_columns(
            pl.struct(cs_secondary)
            .struct.rename_fields(secondary_names)
            .alias("personal_address_secondary"),
        )

    personal_names = [
        c.replace("personal_", "")
        for c in cols
        if c.startswith("personal_") and not c.startswith("personal_address_")
    ]
    if any(primary_names + secondary_names):
        batch = batch.with_columns(
            pl.concat_list(
                cs.by_name("personal_address_primary"),
                cs.by_name("personal_address_secondary"),
            ).alias("personal_addresses"),
        )
        cs_personal = cs_personal | cs.by_name("personal_addresses")
        personal_names.append("addresses")
    if any(personal_names):
        batch = batch.with_columns(
            pl.struct(cs_personal)
            .struct.rename_fields(personal_names)
            .alias("personal"),
        )

    req_pref_names = [
        c.replace("requestPreference_", "")
        for c in cols
        if c.startswith("requestPreference_")
    ]
    if any(req_pref_names):
        batch = batch.with_columns(
            pl.struct(cs_req_pref)
            .struct.rename_fields(req_pref_names)
            .alias("requestPreference"),
        )

    return batch.select(cs.all() - cs_personal - cs_req_pref - cs_addresses)


def run(options: ImportOptions) -> ImportResults:
    """Import users into FOLIO."""
    import_results = ImportResults()
    with Folio(options).connect() as folio:
        for total, b in InputData(options).batch(options.batch_size):
            batch = _transform_batch(b)
            users = [_clean_nones(u) for u in batch.collect().to_dicts()]
            req = {
                "users": users,
                "totalRecords": total,
                "deactivateMissingUsers": options.deactivate_missing_users,
                "updateOnlyPresentFields": not options.update_all_fields,
            }
            if options.source_type:
                req["sourceType"] = options.source_type

            tries = 0
            while tries < 1 + options.retry_count:
                try:
                    res = folio.post_data("/user-import", payload=req)
                    if isinstance(res, int):
                        res_err = f"Expected json but got http code {res}"
                        raise TypeError(res_err)
                    import_results.created_records += int(res["createdRecords"])
                    import_results.failed_records += int(res["failedRecords"])
                    break
                except (httpx.HTTPError, ConnectionError, TimeoutError, RuntimeError):
                    tries = tries + 1
                except (BadRequestError, UnprocessableContentError):
                    tries = 1 + options.retry_count

            if tries == 1 + options.retry_count:
                import_results.failed_records += total

    return import_results
