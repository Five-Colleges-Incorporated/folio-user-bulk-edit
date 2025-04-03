import shlex
import typing
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest
from pytest_cases import parametrize_with_cases

from folio_user_import_manager.commands.check import CheckOptions


@dataclass
class CliArgCase:
    args: str
    envs: dict[str, str]
    _getpass: str
    expected_exception: type[Exception] | type[SystemExit] | None = None
    expected_options: CheckOptions | None = None

    @contextmanager
    def setup(self) -> typing.Any:
        with (
            mock.patch(
                "getpass.getpass",
                return_value=self._getpass,
            ),
            mock.patch.dict("os.environ", self.envs, clear=True),
            mock.patch("pathlib.Path.is_file", return_value=True),
        ):
            yield


_decoy_csv = {"decoy": Path("decoy.csv")}


class CliArgCases:
    def case_args_ok(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -t tenant -u user -p decoy.csv",
            {},
            "pass",
            expected_options=CheckOptions(
                "folio.org",
                "tenant",
                "user",
                "pass",
                _decoy_csv,
            ),
        )

    def case_env_ok(self) -> CliArgCase:
        return CliArgCase(
            "check decoy.csv",
            {
                "FUIMAN__FOLIO__ENDPOINT": "http://folio.org",
                "FUIMAN__FOLIO__TENANT": "tenant",
                "FUIMAN__FOLIO__USERNAME": "user",
                "FUIMAN__FOLIO__PASSWORD": "pass",
            },
            "",
            expected_options=CheckOptions(
                "folio.org",
                "tenant",
                "user",
                "pass",
                _decoy_csv,
            ),
        )

    def case_missing_arg(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -u user -p ./",
            {},
            "pass",
            expected_exception=ValueError,
        )

    def case_bad_arg(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -t -u user -p ./",
            {},
            "pass",
            expected_exception=SystemExit,
        )

    def case_bad_getpass(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -t tenant -u user -p ./",
            {},
            "",
            expected_exception=ValueError,
        )

    def case_env_override(self) -> CliArgCase:
        return CliArgCase(
            "check -u another_user decoy.csv",
            {
                "FUIMAN__FOLIO__ENDPOINT": "http://folio.org",
                "FUIMAN__FOLIO__TENANT": "tenant",
                "FUIMAN__FOLIO__USERNAME": "user",
                "FUIMAN__FOLIO__PASSWORD": "pass",
            },
            "",
            expected_options=CheckOptions(
                "folio.org",
                "tenant",
                "another_user",
                "pass",
                _decoy_csv,
            ),
        )


@mock.patch("folio_user_import_manager.commands.check.run")
@parametrize_with_cases("tc", cases=CliArgCases)
def test_cli_args(
    check_run_mock: mock.Mock,
    tc: CliArgCase,
) -> None:
    import folio_user_import_manager.cli as uut

    with tc.setup():
        if tc.expected_exception is None:
            uut.main(shlex.split(tc.args))
        else:
            with pytest.raises(tc.expected_exception):
                uut.main(shlex.split(tc.args))

    if tc.expected_options is None:
        check_run_mock.assert_not_called()
    else:
        check_run_mock.assert_called_with(tc.expected_options)
