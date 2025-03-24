"""Check that the import can run."""

import http.client
import json
from dataclasses import dataclass


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


def run(options: CheckOptions) -> CheckResults:
    """Checks for connectivity."""
    folio = http.client.HTTPSConnection(options.folio_url)
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

    res = folio.getresponse()
    if res.status == 201 and "folioAccessToken" in res.getheader("set-cookie", ""):
        return CheckResults()

    body = res.read().decode()
    try:
        reason = json.loads(body)["errors"][0].code
    except (ValueError, KeyError):
        reason = body

    return CheckResults(reason)
