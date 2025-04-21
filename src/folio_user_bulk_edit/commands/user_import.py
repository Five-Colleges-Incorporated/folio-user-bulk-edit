"""Command for importing user data into FOLIO."""

import typing
from dataclasses import dataclass, field

import httpx
import polars as pl
import polars.selectors as cs
from pyfolioclient import BadRequestError, UnprocessableContentError

from folio_user_bulk_edit.data import InputData, InputDataOptions
from folio_user_bulk_edit.folio import Folio, FolioOptions


@dataclass(frozen=True)
class ImportOptions(InputDataOptions, FolioOptions):
    """Options used for importing users into FOLIO."""

    batch_size: int
    retry_count: int

    deactivate_missing_users: bool
    update_all_fields: bool
    source_type: str | None


@dataclass
class ImportResults:
    """Results of importing users into FOLIO."""

    created_records: int = 0
    updated_records: int = 0
    failed_records: int = 0
    failed_users: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(
            [],
            schema={
                "username": pl.Utf8,
                "externalSystemId": pl.Utf8,
                "errorMessage": pl.Utf8,
                "source": pl.Utf8,
            },
        ),
    )

    def write_results(self, stream: typing.TextIO) -> None:
        """Pretty prints the results of the check."""
        report = []
        report.append(f"{self.created_records} users created")
        report.append(f"{self.updated_records} users updated")
        report.append(f"{self.failed_records} users failed to create/update")
        report.append("")
        report.append("Sample of failed users")
        report.append("======================")
        report.append(self.failed_users.glimpse(return_as_string=True))

        stream.writelines("\n".join(report) + "\n")


# https://github.com/pola-rs/polars/issues/12795
# Polars doesn't want to add this support for now so we're doing it manually
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
    cs_indiv_addresses = []

    primary_names = [
        c.replace("personal_address_primary_", "")
        for c in cols
        if c.startswith("personal_address_primary_")
    ]
    if any(primary_names):
        cs_indiv_addresses.append(cs.by_name("personal_address_primary"))
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
        cs_indiv_addresses.append(cs.by_name("personal_address_secondary"))
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
    if len(cs_indiv_addresses) > 0:
        batch = batch.with_columns(
            pl.concat_list(*cs_indiv_addresses).alias("personal_addresses"),
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
        for file, total, b in InputData(options).batch(options.batch_size):
            batch = _transform_batch(b).collect()
            users = [_clean_nones(u) for u in batch.to_dicts()]
            req = {
                "users": users,
                "totalRecords": total,
                "deactivateMissingUsers": options.deactivate_missing_users,
                "updateOnlyPresentFields": not options.update_all_fields,
            }
            if options.source_type:
                req["sourceType"] = options.source_type

            last_err: Exception | None = None
            tries = 0
            while tries < 1 + options.retry_count:
                last_err = None
                try:
                    res = folio.post_data("/user-import", payload=req)
                    if isinstance(res, int):
                        res_err = f"Expected json but got http code {res}"
                        raise TypeError(res_err)

                    import_results.created_records += int(res["createdRecords"])
                    import_results.updated_records += int(res["updatedRecords"])
                    import_results.failed_records += int(res["failedRecords"])
                    if any(res.get("failedUsers", [])):
                        import_results.failed_users.vstack(
                            pl.DataFrame(res["failedUsers"]).with_columns(
                                pl.lit(file).alias("source"),
                            ),
                            in_place=True,
                        )

                    break
                except (
                    httpx.HTTPError,
                    ConnectionError,
                    TimeoutError,
                    RuntimeError,
                ) as e:
                    last_err = e
                    tries = tries + 1
                except (BadRequestError, UnprocessableContentError) as e:
                    last_err = e
                    break

            if last_err is not None:
                import_results.failed_records += total
                import_results.failed_users.vstack(
                    batch.select("username", "externalSystemId").with_columns(
                        pl.lit(str(last_err)).alias("errorMessage"),
                        pl.lit(file).alias("source"),
                    ),
                    in_place=True,
                )

    import_results.failed_users = import_results.failed_users.select(
        "source",
        "username",
        "externalSystemId",
        "errorMessage",
    ).rechunk()
    return import_results
