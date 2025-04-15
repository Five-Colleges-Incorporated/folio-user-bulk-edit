"""Input data related utils for managing users."""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandera.polars as pla
import polars as pl

from .schemas import UserImportSchema


@dataclass(frozen=True)
class InputDataOptions(Protocol):
    """Options used for reading input data."""

    data_location: Path | dict[str, Path]


class InputData:
    """The input data as dataframes."""

    def __init__(self, options: InputDataOptions) -> None:
        """Initializes a new instance of InputData."""
        self._options = options

    def test(
        self,
    ) -> tuple[
        dict[str, pla.errors.SchemaErrors] | None,
        dict[str, pl.exceptions.PolarsError] | None,
    ]:
        """Test that connection to FOLIO is ok."""
        schema_errors: dict[str, pla.errors.SchemaErrors] = {}
        read_errors: dict[str, pl.exceptions.PolarsError] = {}

        for n, p in (
            {"data": self._options.data_location}
            if isinstance(self._options.data_location, Path)
            else self._options.data_location
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
