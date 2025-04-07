import http.client
import json
import socket

from ._models import CheckOptions


def run(options: CheckOptions) -> str | None:
    """Checks for connectivity and data validity."""
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
        return "Invalid FOLIO Url"

    res = folio.getresponse()
    if res.status == 201 and "folioAccessToken" in res.getheader("set-cookie", ""):
        return None

    if res.status == 405:
        return "Invalid FOLIO Services Url"

    body = res.read().decode()
    try:
        return str(json.loads(body)["errors"][0]["code"])
    except (ValueError, KeyError):
        return body
