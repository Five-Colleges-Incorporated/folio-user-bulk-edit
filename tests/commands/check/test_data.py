from dataclasses import dataclass
from pathlib import Path

from pandera.polars import errors as ple
from pytest_cases import parametrize, parametrize_with_cases

_samples = list((Path() / "tests" / "commands" / "check" / "samples").glob("*.csv"))


@dataclass
class _SchemaErrors:
    column: str | None = None
    reason: str | ple.SchemaErrorReason | None = None
    check_name: str | None = None


class DataErrorCases:
    @parametrize(csv=[s for s in _samples if "ok" in str(s)])
    def case_ok(self, csv: Path) -> tuple[Path, bool, None]:
        return (csv, True, None)

    @parametrize(csv=[s for s in _samples if "read" in str(s)])
    def case_bad_read(self, csv: Path) -> tuple[Path, bool, None]:
        return (csv, False, None)

    @parametrize(csv=[s for s in _samples if "schema" in str(s)])
    def case_bad_schema(self, csv: Path) -> tuple[Path, bool, _SchemaErrors]:
        with csv.open() as file:
            params = [
                p if len(p) > 0 else None
                for p in file.readline().strip("#").strip("\n").split("|")
            ]
        return (csv, True, _SchemaErrors(*params))


@parametrize_with_cases("path,read_expected,schema_expected", DataErrorCases)
def test_check_data(
    path: Path,
    read_expected: bool,
    schema_expected: _SchemaErrors | None,
) -> None:
    import folio_user_bulk_edit.commands.check as uut

    res = uut.run(
        uut.CheckOptions("", "", "", "", path),
    )

    read_ok = res.read_ok
    assert read_ok == read_expected, (
        str(res.read_errors["data"]) if res.read_errors else None
    )
    schema_ok = res.schema_ok
    assert schema_ok == (schema_expected is None), (
        str(res.schema_errors["data"]) if res.schema_errors else None
    )
    if schema_expected and res.schema_errors:
        assert len(res.schema_errors["data"].schema_errors) == 1
        print(res.schema_errors["data"])  # noqa: T201
        err = res.schema_errors["data"].schema_errors[0]

        if err.data is not None and schema_expected.column is not None:
            assert err.data.column == schema_expected.column

        if err.schema is not None and schema_expected.column is not None:
            assert err.schema.name == schema_expected.column

        if schema_expected.reason:
            assert err.reason_code.name == schema_expected.reason

        if schema_expected.check_name:
            assert err.check.name == schema_expected.check_name
