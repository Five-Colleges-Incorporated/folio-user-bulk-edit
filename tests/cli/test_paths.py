import typing
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

import pytest
from pytest_cases import parametrize_with_cases


@dataclass
class CliPathCase:
    _temp: Path
    input_paths: list[Path]
    expected_exception: type[Exception] | type[SystemExit] | None = None
    expected_paths: Path | dict[str, Path] | None = None

    @contextmanager
    def setup(self) -> typing.Any:
        (self._temp / "d0_f0.csv").touch()
        (self._temp / "d0_f1.csv").touch()

        (self._temp / "d0_d0").mkdir()
        (self._temp / "d0_d0" / "d0_d0_f0.csv").touch()
        (self._temp / "d0_d0" / "d0_d0_f1.csv").touch()

        (self._temp / "d0_d0" / "d0_d0_d0").mkdir()
        (self._temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f0.csv").touch()
        (self._temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f1.csv").touch()

        (self._temp / "d0_d0" / "d0_d0_d1").mkdir()
        (self._temp / "d0_d0" / "d0_d0_d1" / "d0_d0_d1_f0.csv").touch()

        (self._temp / "d0_d1").mkdir()

        with mock.patch.dict(
            "os.environ",
            {
                "UBE__FOLIO__ENDPOINT": "http://folio.org",
                "UBE__FOLIO__TENANT": "tenant",
                "UBE__FOLIO__USERNAME": "user",
                "UBE__FOLIO__PASSWORD": "pass",
            },
            clear=True,
        ):
            yield


class CliPathCases:
    def case_one_file(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [temp / "d0_f0.csv"],
            expected_paths={"d0_f0": temp / "d0_f0.csv"},
        )

    def case_multiple_files(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [temp / "d0_f0.csv", temp / "d0_f1.csv"],
            expected_paths={"d0_f0": temp / "d0_f0.csv", "d0_f1": temp / "d0_f1.csv"},
        )

    def case_one_directory(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [temp / "d0_d0" / "d0_d0_d0"],
            expected_paths={
                "d0_d0_d0_f0": temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f0.csv",
                "d0_d0_d0_f1": temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f1.csv",
            },
        )

    def case_mixed_ok(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [
                temp / "d0_f0.csv",
                temp / "d0_d0" / "d0_d0_d0",
                temp / "d0_d0" / "d0_d0_d1",
            ],
            expected_paths={
                "d0_f0": temp / "d0_f0.csv",
                "d0_d0_d0_f0": temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f0.csv",
                "d0_d0_d0_f1": temp / "d0_d0" / "d0_d0_d0" / "d0_d0_d0_f1.csv",
                "d0_d0_d1_f0": temp / "d0_d0" / "d0_d0_d1" / "d0_d0_d1_f0.csv",
            },
        )

    def case_no_arg(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(temp, [], expected_exception=SystemExit)

    def case_no_files(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(temp, [temp / "d0_d1"], expected_exception=ValueError)

    def case_bad_file(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [temp / "definitely_not_a_file.csv"],
            expected_exception=ValueError,
        )

    def case_bad_directory(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [temp / "definitely_not_a_directory"],
            expected_exception=ValueError,
        )

    def case_mixed_not_ok(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(
            temp,
            [
                temp / "d0_f0.csv",
                temp / "d0_d0" / "d0_d0_d0",
                temp / "d0_d0" / "definitely_not_a_directory",
            ],
            expected_exception=ValueError,
        )


@mock.patch("folio_user_bulk_edit.commands.check.run")
@parametrize_with_cases("tc", cases=CliPathCases)
def test_cli_args(
    check_run_mock: mock.Mock,
    tc: CliPathCase,
) -> None:
    import folio_user_bulk_edit.cli as uut

    with tc.setup():
        if tc.expected_exception is None:
            uut.main(["check"] + [p.as_posix() for p in tc.input_paths])
        else:
            with pytest.raises(tc.expected_exception):
                uut.main(["check"] + [p.as_posix() for p in tc.input_paths])

    if tc.expected_paths is None:
        check_run_mock.assert_not_called()
    else:
        check_run_mock.assert_called_once()
        assert check_run_mock.call_args_list[0][0][0].data_location == tc.expected_paths
