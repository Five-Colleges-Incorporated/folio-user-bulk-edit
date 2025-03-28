"""The Command Line Interface for fuiman."""

import argparse
import os
import sys
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from folio_user_import_manager.commands import check


@dataclass
class _ParsedArgs:
    folio_endpoint: ParseResult | None
    folio_tenant: str | None
    folio_username: str | None
    folio_password: str | None
    ask_folio_password: bool = False
    command: str | None = None
    data: list[Path] | None = None

    @property
    def folio_url(self) -> str | None:
        if self.folio_endpoint is None:
            return None

        return self.folio_endpoint.netloc

    @property
    def data_location(self) -> Path | dict[str, Path] | None:
        if self.data is None or len(self.data) == 0:
            return None

        if len(self.data) == 1:
            return self.data[0]

        return {p.stem: p for p in self.data}


_FUIMAN__FOLIO__ENDPOINT = "FUIMAN__FOLIO__ENDPOINT"
_FUIMAN__FOLIO__TENANT = "FUIMAN__FOLIO__TENANT"
_FUIMAN__FOLIO__USERNAME = "FUIMAN__FOLIO__USERNAME"
_FUIMAN__FOLIO__PASSWORD = "FUIMAN__FOLIO__PASSWORD"  # noqa:S105


def main() -> int:
    """Marshalls inputs and executes commands for fuiman."""
    desc = "Initiates, monitors, and reports on mod-user-import operations in FOLIO"
    parser = argparse.ArgumentParser(prog="fuiman", description=desc)
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    parser.add_argument(
        "command",
        metavar="command",
        choices=["check", "import"],
        help="What action to perform. One of [%(choices)s]",
        type=str,
    )
    parser.add_argument(
        "-e",
        "--folio-endpoint",
        help="Service url of the folio instance. "
        f"Can also be specified as {_FUIMAN__FOLIO__ENDPOINT} environment variable.",
        type=urlparse,
    )
    parser.add_argument(
        "-t",
        "--folio-tenant",
        help="Tenant of the folio instance. "
        f"Can also be specified as {_FUIMAN__FOLIO__TENANT} environment variable.",
        type=str,
    )
    parser.add_argument(
        "-u",
        "--folio-username",
        help="Username of the folio instance service user. "
        f"Can also be specified as {_FUIMAN__FOLIO__USERNAME} environment variable.",
        type=str,
    )
    parser.add_argument(
        "-p",
        "--ask-folio-password",
        action="store_true",
        help="Whether to ask for the password of the folio instance service user. "
        f"Can also be specified as {_FUIMAN__FOLIO__PASSWORD} environment variable.",
    )
    parser.add_argument(
        "data",
        action="extend",
        nargs="+",
        type=Path,
        help="One or more csv files or directories containing csv files to operate on.",
    )

    args = _ParsedArgs(
        urlparse(os.environ[_FUIMAN__FOLIO__ENDPOINT])
        if _FUIMAN__FOLIO__ENDPOINT in os.environ
        else None,
        os.environ.get(_FUIMAN__FOLIO__TENANT),
        os.environ.get(_FUIMAN__FOLIO__USERNAME),
        os.environ.get(_FUIMAN__FOLIO__PASSWORD),
    )
    parser.parse_args(namespace=args)

    if args.ask_folio_password:
        args.folio_password = getpass("FOLIO Password:")

    if any(
        a is None
        for a in [
            args.folio_url,
            args.folio_tenant,
            args.folio_username,
            args.folio_password,
            args.data_location,
        ]
    ):
        parser.print_help()
        return 1

    if args.command == "check":
        # TODO: Figure out how to make this protocol actual work
        check.run(args)  # type: ignore [arg-type]

    return 0


if __name__ == "__main__":
    sys.exit(main())
