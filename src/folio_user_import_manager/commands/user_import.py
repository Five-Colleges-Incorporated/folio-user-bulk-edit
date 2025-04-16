"""Command for importing user data into FOLIO."""

from dataclasses import dataclass

import polars as pl

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


def run(options: ImportOptions) -> ImportResults:
    """Import users into FOLIO."""
    with Folio(options).connect() as folio:
        for total, b in InputData(options).batch(options.batch_size):
            batch = b
            cols = batch.collect_schema().names()
            if "departments" in cols:
                batch = batch.with_columns(pl.col("departments").str.split(","))
            if "customFields" in cols:
                batch = batch.with_columns(pl.col("customFields").str.json_decode())

            users = batch.collect().to_dicts()
            for u in users:
                for k in list(u.keys()):
                    if u[k] is None:
                        del u[k]

            req = {
                "users": users,
                "totalRecords": total,
                "deactivateMissingUsers": options.deactivate_missing_users,
                "updateOnlyPresentFields": not options.update_all_fields,
            }
            if options.source_type:
                req["sourceType"] = options.source_type

            folio.post_data("/user-import", payload=req)

    return ImportResults()
