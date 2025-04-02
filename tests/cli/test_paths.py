import typing
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from pytest_cases import parametrize_with_cases

from folio_user_import_manager.commands.check import CheckOptions


@dataclass
class CliPathCase:
    _temp: Path
    input_paths: list[Path]
    output_paths: Path | dict[str, Path]

    @contextmanager
    def setup(self) -> typing.Any:
        (self._temp / "d0_f0.csv").touch()
        (self._temp / "d0_f1.csv").touch()

        (self._temp / "d0_d0").mkdir()
        (self._temp / "d0_d0" / "d0_d0_f0.csv").touch()
        (self._temp / "d0_d0" / "d0_d0_f1.csv").touch()

        (self._temp / "d0_d0" / "d0_d0_d0").mkdir()
        (self._temp / "d0_d0" / "d0_d0_d0_f0.csv").touch()
        (self._temp / "d0_d0" / "d0_d0_d0_f1.csv").touch()

        (self._temp / "d0_d0" / "d0_d0_d1").mkdir()
        (self._temp / "d0_d0" / "d0_d0_d1_f0.csv").touch()
        (self._temp / "d0_d0" / "d0_d0_d1_f1.csv").touch()

        (self._temp / "d0_d1").mkdir()
        (self._temp / "d0_d1" / "d0_d1_f0.csv").touch()

        with mock.patch.dict(
            "os.environ",
            {
                "FUIMAN__FOLIO__ENDPOINT": "http://folio.org",
                "FUIMAN__FOLIO__TENANT": "tenant",
                "FUIMAN__FOLIO__USERNAME": "user",
                "FUIMAN__FOLIO__PASSWORD": "pass",
            },
            clear=True,
        ):
            yield

    @property
    def expected_options(self) -> CheckOptions:
        return CheckOptions(
            "folio.org",
            "tenant",
            "user",
            "pass",
            self.output_paths,
        )


class CliPathCases:
    def case_one_file(self, tmpdir: str) -> CliPathCase:
        temp = Path(tmpdir)
        return CliPathCase(temp, [temp / "d0_f0.csv"], temp / "d0_f0.csv")


@mock.patch("folio_user_import_manager.commands.check.run")
@parametrize_with_cases("tc", cases=CliPathCases)
def test_cli_args(
    check_run_mock: mock.Mock,
    tc: CliPathCase,
) -> None:
    import folio_user_import_manager.cli as uut

    with tc.setup():
        uut.main(["check"] + [p.as_posix() for p in tc.input_paths])

    check_run_mock.assert_called_with(tc.expected_options)
