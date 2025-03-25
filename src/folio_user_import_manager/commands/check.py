"""Check that the import can run."""

import http.client
import json
import socket
from dataclasses import dataclass

import pandera.polars as pla


@dataclass
class CheckOptions:
    """Options used for checking an import's viability."""

    folio_url: str
    folio_tenant: str
    folio_username: str
    folio_password: str


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
    def data_ok(self) -> bool:
        """Is the data valid?"""
        return self.data_errors is None

    """The errors (if there are any) with the data."""
    data_errors: pla.errors.SchemaErrors | None = None


def run(options: CheckOptions) -> CheckResults:
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

    data_errors = None
    while True:
        break

    return CheckResults(folio_error, data_errors)
