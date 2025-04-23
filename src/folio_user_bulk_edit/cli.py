"""The Command Line Interface for fuiman."""

import argparse
import getpass
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import ParseResult, urlparse, urlunparse

from folio_user_bulk_edit import _cli_log
from folio_user_bulk_edit.commands import check, user_import

_FOLIO__ENDPOINT = "UBE__FOLIO__ENDPOINT"
_FOLIO__TENANT = "UBE__FOLIO__TENANT"
_FOLIO__USERNAME = "UBE__FOLIO__USERNAME"
_FOLIO__PASSWORD = "UBE__FOLIO__PASSWORD"  # noqa:S105

_BATCH__BATCHSIZE = "UBE__BATCHSETTINGS__BATCHSIZE"
_BATCH__RETRYCOUNT = "UBE__BATCHSETTINGS__RETRYCOUNT"

_MODUSERIMPORT__DEACTIVATEMISSINGUSERS = "UBE__MODUSERIMPORT__DEACTIVATEMISSINGUSERS"
_MODUSERIMPORT__UPDATEALLFIELDS = "UBE__MODUSERIMPORT__UPDATEALLFIELDS"
_MODUSERIMPORT__SOURCETYPE = "UBE__MODUSERIMPORT__SOURCETYPE"


def _url_param(param: str) -> ParseResult:
    # Following the syntax specifications in RFC 1808,
    # urlparse recognizes a netloc only if it is properly introduced by '//'
    if "//" not in param:
        param = "//" + param
    return urlparse(param, scheme="https")


@dataclass
class _ParsedArgs:
    # These have internal defaults, env vars, and cli flags
    batch_size: int
    retry_count: int
    default_deactivate_missing_users: bool
    default_update_all_fields: bool

    # These have env vars and cli flags
    folio_endpoint: ParseResult | None = None
    folio_tenant: str | None = None
    folio_username: str | None = None
    folio_password: str | None = None
    ask_folio_password: bool = False

    source_type: str | None = None

    # the subparser
    command: str | None = None

    # These boolean flags don't behave like the rest of the fields
    deactivate_missing_users: bool | None = None
    update_all_fields: bool | None = None

    # see note below on nargs + subparsers
    additional_data: list[Path] | None = None
    data: Path | None = None

    # these have just defaults and cli flags
    verbose: int = 0
    log_directory: Path = Path("./logs")

    @property
    def folio_url(self) -> str | None:
        if self.folio_endpoint is None:
            return None

        return urlunparse(self.folio_endpoint[:2] + ("", "", None, None))

    @property
    def data_location(self) -> Path | dict[str, Path] | None:
        if self.data is None:
            return None

        all_data = [self.data]
        if self.additional_data is not None:
            all_data = all_data + self.additional_data

        locations: dict[str, Path] = {}
        for p in all_data:
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

    def as_import_options(self) -> user_import.ImportOptions:
        if (
            self.folio_url is None
            or self.folio_tenant is None
            or self.folio_username is None
            or self.folio_password is None
            or self.data_location is None
        ):
            none = "One or more required options is missing"
            raise ValueError(none)

        return user_import.ImportOptions(
            self.folio_url,
            self.folio_tenant,
            self.folio_username,
            self.folio_password,
            self.data_location,
            self.batch_size,
            self.retry_count,
            self.default_deactivate_missing_users
            if self.deactivate_missing_users is None
            else self.deactivate_missing_users,
            self.default_update_all_fields
            if self.update_all_fields is None
            else self.update_all_fields,
            self.source_type,
        )

    @staticmethod
    @lru_cache
    def parser() -> argparse.ArgumentParser:
        desc = "Initiates, monitors, and reports on bulk user operations in FOLIO."
        parser = argparse.ArgumentParser(prog="ube", description=desc)

        # Make sure to also update the project
        parser.add_argument("--version", action="version", version="%(prog)s 1.0.0")

        parser.add_argument("-v", "--verbose", action="count")
        parser.add_argument("--log-directory", type=Path)

        folio_parser = parser.add_argument_group("FOLIO Settings")
        folio_parser.add_argument(
            "-e",
            "--folio-endpoint",
            help="Service url of the FOLIO instance. "
            f"Can also be specified as {_FOLIO__ENDPOINT} environment variable.",
            type=_url_param,
        )
        folio_parser.add_argument(
            "-t",
            "--folio-tenant",
            help="Tenant of the FOLIO instance. "
            f"Can also be specified as {_FOLIO__TENANT} environment variable.",
            type=str,
        )
        folio_parser.add_argument(
            "-u",
            "--folio-username",
            help="Username of the FOLIO instance service user. "
            f"Can also be specified as {_FOLIO__USERNAME} environment variable.",
            type=str,
        )
        folio_parser.add_argument(
            "-p",
            "--ask-folio-password",
            action="store_true",
            help="Whether to ask for the password of the FOLIO instance service user. "
            f"Can also be specified as {_FOLIO__PASSWORD} environment variable.",
        )

        folio_parser = parser.add_argument_group("Batch Settings")
        folio_parser.add_argument(
            "--batch-size",
            help="Maximum number of records to send to FOLIO at a time. "
            f"Can also be specified as {_BATCH__BATCHSIZE} environment variable.",
            type=int,
        )
        folio_parser.add_argument(
            "--retry-count",
            help="Maximum number times a failed request can be retried. "
            f"Can also be specified as {_BATCH__RETRYCOUNT} environment variable.",
            type=int,
        )

        data_desc = "One or more .csvs or directories with .csvs to operate on."

        def data(p: argparse.ArgumentParser) -> None:
            p.add_argument(
                "additional_data",
                action="extend",
                nargs="*",
                metavar="data",
                type=Path,
                help=data_desc,
            )

        commands = parser.add_subparsers(dest="command", metavar="command")
        check_desc = "Quickly checks input files and FOLIO connection for validity."
        check_parser = commands.add_parser(
            "check",
            help=check_desc,
            description=check_desc,
        )

        import_desc = "Imports input files to FOLIO and reports on progress and errors."
        import_parser = commands.add_parser(
            "import",
            help=import_desc,
            description=import_desc,
        )
        import_parser.add_argument(
            "--deactivate-missing-users",
            action=argparse.BooleanOptionalAction,
            help="Indicates whether to deactivate users "
            "that are missing in current user's data collection. "
            f"Can also be specified as {_MODUSERIMPORT__DEACTIVATEMISSINGUSERS} "
            "environment variable.",
        )
        import_parser.add_argument(
            "--update-all-fields",
            action=argparse.BooleanOptionalAction,
            help="Indicates whether to update only present fields in user's data. "
            "Currently this only works for addresses. "
            f"Can also be specified as {_MODUSERIMPORT__UPDATEALLFIELDS} "
            "environment variable.",
        )
        folio_parser.add_argument(
            "--source-type",
            help="A prefix for the externalSystemId. "
            f"Can also be specified as {_MODUSERIMPORT__SOURCETYPE} "
            "environment variable.",
            type=str,
        )

        # https://stackoverflow.com/a/74492728
        # subparsers interact poorly with nargs
        # we have a somewhat dummy path arg here to display properly in help
        data(check_parser)
        data(import_parser)
        parser.add_argument(
            "data",
            type=Path,
            help=data_desc,
        )

        return parser


def main(args: list[str] | None = None) -> None:
    """Marshalls inputs and executes commands for fuiman."""
    parsed_args = _ParsedArgs(
        folio_endpoint=_url_param(os.environ[_FOLIO__ENDPOINT])
        if _FOLIO__ENDPOINT in os.environ
        else None,
        folio_tenant=os.environ.get(_FOLIO__TENANT),
        folio_username=os.environ.get(_FOLIO__USERNAME),
        folio_password=os.environ.get(_FOLIO__PASSWORD),
        batch_size=int(os.environ.get(_BATCH__BATCHSIZE, "1000")),
        retry_count=int(os.environ.get(_BATCH__RETRYCOUNT, "1")),
        default_deactivate_missing_users=os.environ.get(
            _MODUSERIMPORT__DEACTIVATEMISSINGUSERS,
            "0",
        )
        == "1",
        default_update_all_fields=os.environ.get(
            _MODUSERIMPORT__UPDATEALLFIELDS,
            "0",
        )
        == "1",
        source_type=os.environ.get(_MODUSERIMPORT__SOURCETYPE),
    )
    parser = _ParsedArgs.parser()
    parsed_args = parser.parse_args(args, namespace=parsed_args)
    now = datetime.now(tz=None).strftime("%y%m%d%H%M%S")  # noqa: DTZ005

    _cli_log.initialize(
        parsed_args.log_directory,
        now,
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
            c_opts = parsed_args.as_check_options()
        except ValueError:
            parser.print_usage()
            raise
        check.run(c_opts).write_results(sys.stdout)
    elif parsed_args.command == "import":
        try:
            i_opts = parsed_args.as_import_options()
        except ValueError:
            parser.print_usage()
            raise
        results = user_import.run(i_opts)
        results.failed_users.write_csv(
            parsed_args.log_directory / f"{now}-failedUsers.csv",
        )
        results.write_results(sys.stdout)


if __name__ == "__main__":
    main()
