"""The Command Line Interface for fuiman."""

import argparse
import getpass
import os
import sys
from dataclasses import dataclass
from functools import lru_cache, partial
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from folio_user_import_manager import _cli_log
from folio_user_import_manager.commands import check

_FOLIO__ENDPOINT = "FUIMAN__FOLIO__ENDPOINT"
_FOLIO__TENANT = "FUIMAN__FOLIO__TENANT"
_FOLIO__USERNAME = "FUIMAN__FOLIO__USERNAME"
_FOLIO__PASSWORD = "FUIMAN__FOLIO__PASSWORD"  # noqa:S105


@dataclass
class _ParsedArgs:
    folio_endpoint: ParseResult | None
    folio_tenant: str | None
    folio_username: str | None
    folio_password: str | None
    ask_folio_password: bool = False
    command: str | None = None
    data: list[Path] | None = None
    verbose: int = 0
    log_directory: Path = Path("./logs")

    @property
    def folio_url(self) -> str | None:
        if self.folio_endpoint is None:
            return None

        return self.folio_endpoint.netloc

    @property
    def data_location(self) -> Path | dict[str, Path] | None:
        if self.data is None or len(self.data) == 0:
            return None

        locations: dict[str, Path] = {}
        for p in self.data:
            if p.is_file():
                locations[p.stem] = p
                continue

            if not p.is_file() and not p.is_dir():
                file = f"{p.resolve().absolute()} does not exist or isn't readable"
                raise ValueError(file)

            locations = locations | {sp.stem: sp for sp in p.glob("**/*.csv")}

        return locations if len(locations) > 0 else None

    def as_check_options(self) -> check.CheckOptions:
        if (
            self.folio_url is None
            or self.folio_tenant is None
            or self.folio_username is None
            or self.folio_password is None
            or self.data_location is None
        ):
            none = "One or more required options is missing"
            raise ValueError(none)

        return check.CheckOptions(
            self.folio_url,
            self.folio_tenant,
            self.folio_username,
            self.folio_password,
            self.data_location,
        )

    @staticmethod
    @lru_cache
    def parser() -> argparse.ArgumentParser:
        desc = "Initiates, monitors, and reports on mod-user-import operations in FOLIO"
        parser = argparse.ArgumentParser(prog="fuiman", description=desc)

        parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
        parser.add_argument("--log-directory", type=Path)
        parser.add_argument("-v", "--verbose", action="count")

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
            f"Can also be specified as {_FOLIO__ENDPOINT} environment variable.",
            type=partial(urlparse, scheme="https"),
        )
        parser.add_argument(
            "-t",
            "--folio-tenant",
            help="Tenant of the folio instance. "
            f"Can also be specified as {_FOLIO__TENANT} environment variable.",
            type=str,
        )
        parser.add_argument(
            "-u",
            "--folio-username",
            help="Username of the folio instance service user. "
            f"Can also be specified as {_FOLIO__USERNAME} environment variable.",
            type=str,
        )
        parser.add_argument(
            "-p",
            "--ask-folio-password",
            action="store_true",
            help="Whether to ask for the password of the folio instance service user. "
            f"Can also be specified as {_FOLIO__PASSWORD} environment variable.",
        )
        parser.add_argument(
            "data",
            action="extend",
            nargs="+",
            type=Path,
            help="One or more .csvs or directories with .csvs to operate on.",
        )
        return parser


def main(args: list[str] | None = None) -> None:
    """Marshalls inputs and executes commands for fuiman."""
    parsed_args = _ParsedArgs(
        urlparse(os.environ[_FOLIO__ENDPOINT], scheme="https://")
        if _FOLIO__ENDPOINT in os.environ
        else None,
        os.environ.get(_FOLIO__TENANT),
        os.environ.get(_FOLIO__USERNAME),
        os.environ.get(_FOLIO__PASSWORD),
    )
    parser = _ParsedArgs.parser()
    parsed_args = _ParsedArgs.parser().parse_args(args, namespace=parsed_args)
    _cli_log.initialize(
        parsed_args.log_directory,
        30 - (parsed_args.verbose * 10),
        20 - (min(1, parsed_args.verbose) * 10),
    )

    if parsed_args.ask_folio_password:
        parsed_args.folio_password = getpass.getpass("FOLIO Password:")
        if len(parsed_args.folio_password) == 0:
            empty = "FOLIO Password is required"
            parser.print_usage()
            raise ValueError(empty)

    if parsed_args.command == "check":
        try:
            opts = parsed_args.as_check_options()
        except ValueError:
            parser.print_usage()
            raise
        check.run(opts).write_results(sys.stdout)


if __name__ == "__main__":
    main()
