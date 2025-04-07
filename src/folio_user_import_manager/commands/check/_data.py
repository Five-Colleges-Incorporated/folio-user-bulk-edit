import typing
from pathlib import Path

import pandera.polars as pla
import polars as pl
from pandera.engines.polars_engine import DateTime

from ._models import CheckOptions

# https://dev.folio.org/guides/uuids/
_FOLIO_UUID = r""
r"^[a-fA-F0-9]{8}-"
r"[a-fA-F0-9]{4}-"
r"[1-5][a-fA-F0-9]{3}-"
r"[89abAB][a-fA-F0-9]{3}-"
r"[a-fA-F0-9]{12}$"


def val_limited_is_unique_vector(
    vals: list[str] | None = None,
) -> typing.Callable[[pla.PolarsData], pl.LazyFrame]:
    def val_filter(data: pla.PolarsData) -> pl.LazyFrame:
        if data.key is None:
            return data.lazyframe.select(pl.lit(value=False))

        vec = data.lazyframe.select(
            pl.col(data.key),
            pl.col(data.key).list.len().alias("n"),
            pl.col(data.key).list.n_unique().alias("n_unique"),
            pl.col("n_unique").eq(pl.col("n").alias("ok")),
        )
        if vals:
            vec = vec.select(
                pl.Expr.and_(
                    pl.col("ok"),
                    pl.col("n_unique").le(pl.lit(len(vals))),
                    pl.col(data.key)
                    .list.set_intersection(pl.lit(vals))
                    .len()
                    .eq(pl.col(data.key).len()),
                ),
            )

        return vec.select("ok")

    return val_filter


def run(
    options: CheckOptions,
) -> tuple[
    dict[str, pla.errors.SchemaErrors] | None,
    dict[str, pl.exceptions.PolarsError] | None,
]:
    schema_errors: dict[str, pla.errors.SchemaErrors] = {}
    read_errors: dict[str, pl.exceptions.PolarsError] = {}
    user_data_import_schema = pla.DataFrameSchema(
        {
            "username": pla.Column(
                str,
                description="A unique name belonging to a user. "
                "Typically used for login",
                unique=True,
            ),
            "externalSystemId": pla.Column(
                str,
                description="A unique ID that corresponds to an external authority",
                unique=True,
            ),
            "id": pla.Column(
                str,
                description="A globally unique (UUID) identifier for the user",
                unique=True,
                required=False,
                nullable=True,
                checks=[pla.Check.str_matches(_FOLIO_UUID)],
            ),
            "barcode": pla.Column(
                str,
                description="The unique library barcode for this user",
                unique=True,
                required=False,
                nullable=True,
            ),
            "active": pla.Column(
                bool,
                description="A flag to determine "
                "if the user's account is effective and not expired. "
                "The tenant configuration can require the user to be active for login. "
                "Active is different from the loan patron block",
                required=False,
                nullable=True,
            ),
            "type": pla.Column(
                str,
                description="The class of user like staff or patron; "
                "this is different from patronGroup; "
                "it can store shadow, system user and dcb types also",
                required=False,
                nullable=True,
                checks=[pla.Check.isin(["Patron", "Staff"])],
            ),
            "patronGroup": pla.Column(
                str,
                description="The name of the patron group the user belongs to; "
                "this is different from the patronGroup property of the /users API "
                "that is a UUID.",
                required=False,
                nullable=True,
            ),
            "departments": pla.Column(
                pl.List(pl.Utf8()),  # type: ignore[arg-type]
                description="List of names of the departments the user belongs to; "
                "this is different from the departments property of the /users API"
                "this is a UUID.",  # This seems like incorrect docs
                required=False,
                nullable=True,
                checks=[pla.Check(val_limited_is_unique_vector())],
            ),
            "enrollmentDate": pla.Column(
                DateTime(time_zone_agnostic=True),  # type: ignore[arg-type]
                description="The date in which the user joined the organization",
                required=False,
                nullable=True,
            ),
            "expirationDate": pla.Column(
                DateTime(time_zone_agnostic=True),  # type: ignore[arg-type]
                description="The date for when the user becomes inactive",
                required=False,
                nullable=True,
            ),
            "preferredEmailCommunication": pla.Column(
                pl.List(pl.Utf8()),  # type: ignore[arg-type]
                description="Preferred email communication types",
                required=False,
                nullable=True,
                checks=[
                    pla.Check(
                        val_limited_is_unique_vector(
                            ["Support", "Programs", "Services"],
                        ),
                    ),
                ],
            ),
            "meta": pla.Column(
                required=False,
                nullable=False,
                checks=[pla.Check(lambda _: False, name="Deprecated")],
            ),
            "proxyFor": pla.Column(
                required=False,
                nullable=False,
                checks=[pla.Check(lambda _: False, name="Deprecated")],
            ),
            "createdDate": pla.Column(
                required=False,
                nullable=False,
                checks=[pla.Check(lambda _: False, name="Deprecated")],
            ),
            "updatedDate": pla.Column(
                required=False,
                nullable=False,
                checks=[pla.Check(lambda _: False, name="Deprecated")],
            ),
        },
        strict=False,
    )

    for n, p in (
        {"data": options.data_location}
        if isinstance(options.data_location, Path)
        else options.data_location
    ).items():
        try:
            pl.read_csv(p)
        except pl.exceptions.PolarsError as e:
            read_errors[n] = e

        data: pl.DataFrame | None
        try:
            data = pl.read_csv(p, ignore_errors=True)
        except pl.exceptions.PolarsError as e:
            if n not in read_errors:
                read_errors[n] = e
            continue

        try:
            user_data_import_schema.validate(data, lazy=True)
        except pla.errors.SchemaError as se:
            schema_errors[n] = pla.errors.SchemaErrors(
                user_data_import_schema,
                [se],
                data,
            )
        except pla.errors.SchemaErrors as se:
            schema_errors[n] = se

    return (
        schema_errors if len(schema_errors) > 0 else None,
        read_errors if len(read_errors) > 0 else None,
    )
