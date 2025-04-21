import shlex
import typing
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest
from pytest_cases import parametrize_with_cases

from folio_user_bulk_edit.commands.check import CheckOptions
from folio_user_bulk_edit.commands.user_import import ImportOptions


@dataclass
class CliArgCase:
    args: str
    envs: dict[str, str]
    _getpass: str
    expected_exception: type[Exception] | type[SystemExit] | None = None
    expected_options: CheckOptions | ImportOptions | None = None

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
            "-e http://folio.org -t tenant -u user -p check decoy.csv",
            {},
            "pass",
            expected_options=CheckOptions(
                "http://folio.org",
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
                "UBE__FOLIO__ENDPOINT": "http://folio.org",
                "UBE__FOLIO__TENANT": "tenant",
                "UBE__FOLIO__USERNAME": "user",
                "UBE__FOLIO__PASSWORD": "pass",
            },
            "",
            expected_options=CheckOptions(
                "http://folio.org",
                "tenant",
                "user",
                "pass",
                _decoy_csv,
            ),
        )

    def case_missing_arg(self) -> CliArgCase:
        return CliArgCase(
            "-e http://folio.org -u user -p check ./",
            {},
            "pass",
            expected_exception=ValueError,
        )

    def case_bad_arg(self) -> CliArgCase:
        return CliArgCase(
            "-e http://folio.org -t -u user -p check ./",
            {},
            "pass",
            expected_exception=SystemExit,
        )

    def case_bad_getpass(self) -> CliArgCase:
        return CliArgCase(
            "-e http://folio.org -t tenant -u user -p check ./",
            {},
            "",
            expected_exception=ValueError,
        )

    def case_env_override(self) -> CliArgCase:
        return CliArgCase(
            "-u another_user check decoy.csv",
            {
                "UBE__FOLIO__ENDPOINT": "http://folio.org",
                "UBE__FOLIO__TENANT": "tenant",
                "UBE__FOLIO__USERNAME": "user",
                "UBE__FOLIO__PASSWORD": "pass",
            },
            "",
            expected_options=CheckOptions(
                "http://folio.org",
                "tenant",
                "another_user",
                "pass",
                _decoy_csv,
            ),
        )

    def case_import_overrides(self) -> CliArgCase:
        return CliArgCase(
            "--batch-size 2 import --no-update-all-fields decoy.csv",
            {
                "UBE__FOLIO__ENDPOINT": "http://folio.org",
                "UBE__FOLIO__TENANT": "tenant",
                "UBE__FOLIO__USERNAME": "user",
                "UBE__FOLIO__PASSWORD": "pass",
                "UBE__BATCHSETTINGS__BATCHSIZE": "1",
                "UBE__MODUSERIMPORT__DEACTIVATEMISSINGUSERS": "1",
                "UBE__MODUSERIMPORT__UPDATEALLFIELDS": "1",
            },
            "",
            expected_options=ImportOptions(
                "http://folio.org",
                "tenant",
                "user",
                "pass",
                _decoy_csv,
                2,
                1,
                True,
                False,
                None,
            ),
        )

    def case_default_scheme(self) -> CliArgCase:
        return CliArgCase(
            "-e folio.org -t tenant -u user -p check decoy.csv",
            {},
            "pass",
            expected_options=CheckOptions(
                "https://folio.org",
                "tenant",
                "user",
                "pass",
                _decoy_csv,
            ),
        )


@mock.patch("folio_user_bulk_edit.commands.user_import.run")
@mock.patch("folio_user_bulk_edit.commands.check.run")
@parametrize_with_cases("tc", cases=CliArgCases)
def test_cli_args(
    check_mock: mock.Mock,
    import_mock: mock.Mock,
    tc: CliArgCase,
) -> None:
    import folio_user_bulk_edit.cli as uut

    with tc.setup():
        if tc.expected_exception is None:
            uut.main(shlex.split(tc.args))
        else:
            with pytest.raises(tc.expected_exception):
                uut.main(shlex.split(tc.args))

    if tc.expected_options is None:
        check_mock.assert_not_called()
        import_mock.assert_not_called()
        return

    if isinstance(tc.expected_options, CheckOptions):
        check_mock.assert_called_with(tc.expected_options)
    elif isinstance(tc.expected_options, ImportOptions):
        import_mock.assert_called_with(tc.expected_options)
    else:
        pytest.fail(f"Unknown result type {tc.expected_options}")
