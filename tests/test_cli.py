import shlex
import typing
from dataclasses import dataclass
from unittest import mock

from pytest_cases import parametrize_with_cases


@dataclass
class CliArgCase:
    args: str
    _getpass: str
    expected_result: int
    expected_calls: int

    def patch_getpass(self) -> typing.Any:
        return mock.patch(
            "getpass.getpass",
            return_value=self._getpass,
        )


class CliArgCases:
    def case_ok(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -t tenant -u user -p ./",
            "pass",
            0,
            1,
        )


@mock.patch("folio_user_import_manager.commands.check.run")
@parametrize_with_cases("tc", cases=CliArgCases)
def test_cli_args(
    check_run_mock: mock.Mock,
    tc: CliArgCase,
) -> None:
    import folio_user_import_manager.cli as uut

    with tc.patch_getpass():
        res = uut.main(shlex.split(tc.args))

    assert res == tc.expected_result
    assert check_run_mock.call_count == tc.expected_calls
