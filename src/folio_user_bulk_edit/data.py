"""Input data related utils for managing users."""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandera.polars as pla
import polars as pl

from .schemas import UserImportSchema


@dataclass(frozen=True)
class InputDataOptions:
    """Options used for reading input data."""

    data_location: Path | dict[str, Path]


class InputData:
    """The input data as dataframes."""

    def __init__(self, options: InputDataOptions) -> None:
        """Initializes a new instance of InputData."""
        self._options = options

    @classmethod
    def _scan_csv(cls, path: Path, ignore_errors: bool = False) -> pl.LazyFrame:
        return pl.scan_csv(
            path,
            comment_prefix="#",
            ignore_errors=ignore_errors,
            try_parse_dates=True,
            schema_overrides={"barcode": pl.Utf8},
        )

    def batch(
        self,
        batch_size: int,
    ) -> Iterator[tuple[str, int, pl.LazyFrame]]:
        """Streams input data in batches up to batch_size."""
        for f, p in (
            {"data": self._options.data_location}
            if isinstance(self._options.data_location, Path)
            else self._options.data_location
        ).items():
            data = self._scan_csv(p).with_row_index()

            batch_num = 0
            rows_batched = batch_size
            while rows_batched == batch_size:
                batch = data.filter(
                    pl.Expr.and_(
                        pl.col("index").ge(pl.lit(batch_size * batch_num)),
                        pl.col("index").lt(pl.lit(batch_size * (batch_num + 1))),
                    ),
                )

                batch_num += 1
                rows_batched = int(batch.select(pl.len()).collect().item())
                yield (f, rows_batched, batch.drop("index"))

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
                self._scan_csv(p).collect()
            except pl.exceptions.PolarsError as e:
                read_errors[n] = e

            data: pl.DataFrame | None
            try:
                data = self._scan_csv(p, ignore_errors=True).collect()
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
