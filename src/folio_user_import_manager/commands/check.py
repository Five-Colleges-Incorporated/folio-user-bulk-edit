"""Check that the import can run."""

import http.client
import json
import socket
from dataclasses import dataclass
from pathlib import Path

import pandera.polars as pla
import polars as pl


@dataclass
class CheckOptions:
    """Options used for checking an import's viability."""

    folio_url: str
    folio_tenant: str
    folio_username: str
    folio_password: str

    data_location: Path | dict[str, Path]


@dataclass
class CheckResults:
    """Results of checking an import's viablity."""

    @property
    def folio_ok(self) -> bool:
        """Is the connection to FOLIO ok?"""
        return self.folio_error is None

    """The error (if there is one) connecting to FOLIO during the check."""
    folio_error: str | None = None

    @property
    def schema_ok(self) -> bool:
        """Is the data valid?"""
        return self.schema_errors is None

    """The errors (if there are any) with the validity of the data."""
    schema_errors: dict[str, pla.errors.SchemaErrors] | None = None

    @property
    def read_ok(self) -> bool:
        """Can we read the data as a csv?"""
        return self.read_errors is None

    """The errors (if there are any) encountered reading the data."""
    read_errors: dict[str, pl.exceptions.PolarsError] | None = None


def run(options: CheckOptions) -> CheckResults:  # noqa: C901 (to be broken out after testing)
    """Checks for connectivity and data validity."""
    folio_error = None
    while True:
        folio = http.client.HTTPSConnection(options.folio_url)
        try:
            folio.request(
                "POST",
                "/authn/login-with-expiry",
                json.dumps(
                    {
                        "username": options.folio_username,
                        "password": options.folio_password,
                    },
                ),
                {
                    "x-okapi-tenant": options.folio_tenant,
                    "content-type": "application/json",
                },
            )
        except socket.gaierror:
            folio_error = "Invalid FOLIO Url"
            break

        res = folio.getresponse()
        if res.status == 201 and "folioAccessToken" in res.getheader("set-cookie", ""):
            break

        if res.status == 405:
            folio_error = "Invalid FOLIO Services Url"
            break

        body = res.read().decode()
        try:
            folio_error = json.loads(body)["errors"][0]["code"]
        except (ValueError, KeyError):
            folio_error = body

        break

    schema_errors: dict[str, pla.errors.SchemaErrors] = {}
    read_errors: dict[str, pl.exceptions.PolarsError] = {}
    while True:
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
            },
            strict=True,
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

        break

    return CheckResults(
        folio_error,
        schema_errors if len(schema_errors) > 0 else None,
        read_errors if len(read_errors) > 0 else None,
    )
