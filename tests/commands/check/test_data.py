from pathlib import Path

from pytest_cases import parametrize, parametrize_with_cases

_samples = list((Path() / "tests" / "commands" / "check" / "samples").glob("*.csv"))


class DataErrorCases:
    @parametrize(csv=[s for s in _samples if "ok" in str(s)])
    def case_ok(self, csv: Path) -> tuple[Path, bool, bool]:
        return (csv, True, True)

    @parametrize(csv=[s for s in _samples if "read" in str(s)])
    def case_bad_read(self, csv: Path) -> tuple[Path, bool, bool]:
        return (csv, True, False)

    @parametrize(csv=[s for s in _samples if "schema" in str(s)])
    def case_bad_schema(self, csv: Path) -> tuple[Path, bool, bool]:
        return (csv, False, True)


@parametrize_with_cases("path,read_ok,schema_ok", DataErrorCases)
def test_check_data(path: Path, read_ok: bool, schema_ok: bool) -> None:  # noqa: FBT001
    import folio_user_import_manager.commands.check as uut

    res = uut.run(
        uut.CheckOptions("", "", "", "", path),
    )

    assert res.read_ok == read_ok, (
        str(res.read_errors["data"]) if res.read_errors else None
    )
    assert res.schema_ok == schema_ok, (
        str(res.schema_errors["data"]) if res.schema_errors else None
    )
