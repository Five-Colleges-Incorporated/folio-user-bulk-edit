from pathlib import Path

import pandera.polars as pla
import polars as pl

from ._models import CheckOptions
from ._schema import UserImportSchema


def run(
    options: CheckOptions,
) -> tuple[
    dict[str, pla.errors.SchemaErrors] | None,
    dict[str, pl.exceptions.PolarsError] | None,
]:
    schema_errors: dict[str, pla.errors.SchemaErrors] = {}
    read_errors: dict[str, pl.exceptions.PolarsError] = {}

    for n, p in (
        {"data": options.data_location}
        if isinstance(options.data_location, Path)
        else options.data_location
    ).items():
        try:
            pl.read_csv(p, comment_prefix="#", try_parse_dates=True)
        except pl.exceptions.PolarsError as e:
            read_errors[n] = e

        data: pl.DataFrame | None
        try:
            data = pl.read_csv(
                p,
                comment_prefix="#",
                ignore_errors=True,
                try_parse_dates=True,
            )
        except pl.exceptions.PolarsError as e:
            if n not in read_errors:
                read_errors[n] = e
            continue

        try:
            UserImportSchema.validate(data, lazy=True)
        except pla.errors.SchemaError as se:
            schema_errors[n] = pla.errors.SchemaErrors(
                UserImportSchema.to_schema(),
                [se],
                data,
            )
        except pla.errors.SchemaErrors as se:
            schema_errors[n] = se

    return (
        schema_errors if len(schema_errors) > 0 else None,
        read_errors if len(read_errors) > 0 else None,
    )
