import shlex
from unittest import mock


@mock.patch("getpass.getpass")
@mock.patch("folio_user_import_manager.commands.check.run")
def test_ok(getpass_mock: mock.Mock, check_run_mock: mock.Mock) -> None:
    import folio_user_import_manager.cli as uut

    getpass_mock.return_value = "pass"
    res = uut.main(shlex.split("check -e http://folio.org -t tenant -u user -p ./"))

    assert res == 0
    check_run_mock.assert_called_once()
