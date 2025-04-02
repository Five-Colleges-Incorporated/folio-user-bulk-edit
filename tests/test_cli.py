import shlex
import typing
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from pytest_cases import parametrize_with_cases

from folio_user_import_manager.commands.check import CheckOptions


@dataclass
class CliArgCase:
    args: str
    envs: dict[str, str]
    _getpass: str
    expected_result: int
    expected_options: CheckOptions

    @contextmanager
    def setup(self) -> typing.Any:
        with (
            mock.patch(
                "getpass.getpass",
                return_value=self._getpass,
            ),
            mock.patch.dict("os.environ", self.envs, clear=True),
        ):
            yield


class CliArgCases:
    def case_args_ok(self) -> CliArgCase:
        return CliArgCase(
            "check -e http://folio.org -t tenant -u user -p ./",
            {},
            "pass",
            0,
            CheckOptions("folio.org", "tenant", "user", "pass", Path("./")),
        )

    def case_env_ok(self) -> CliArgCase:
        return CliArgCase(
            "check ./",
            {
                "FUIMAN__FOLIO__ENDPOINT": "http://folio.org",
                "FUIMAN__FOLIO__TENANT": "tenant",
                "FUIMAN__FOLIO__USERNAME": "user",
                "FUIMAN__FOLIO__PASSWORD": "pass",
            },
            "",
            0,
            CheckOptions("folio.org", "tenant", "user", "pass", Path("./")),
        )

    def case_env_override(self) -> CliArgCase:
        return CliArgCase(
            "check -u another_user ./",
            {
                "FUIMAN__FOLIO__ENDPOINT": "http://folio.org",
                "FUIMAN__FOLIO__TENANT": "tenant",
                "FUIMAN__FOLIO__USERNAME": "user",
                "FUIMAN__FOLIO__PASSWORD": "pass",
            },
            "",
            0,
            CheckOptions("folio.org", "tenant", "another_user", "pass", Path("./")),
        )


@mock.patch("folio_user_import_manager.commands.check.run")
@parametrize_with_cases("tc", cases=CliArgCases)
def test_cli_args(
    check_run_mock: mock.Mock,
    tc: CliArgCase,
) -> None:
    import folio_user_import_manager.cli as uut

    with tc.setup():
        res = uut.main(shlex.split(tc.args))

    assert res == tc.expected_result
    check_run_mock.assert_called_with(tc.expected_options)
