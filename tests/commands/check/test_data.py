import pytest


def test_check_data_ok(request: pytest.FixtureRequest) -> None:
    import folio_user_import_manager.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            "",
            "",
            "",
            "",
            data_location=request.path.parent / "samples" / "ok.csv",
        ),
    )

    assert res.read_ok, str(res.read_errors["data"]) if res.read_errors else None
    assert res.schema_ok, str(res.schema_errors["data"]) if res.schema_errors else None


def test_check_schema_not_ok(request: pytest.FixtureRequest) -> None:
    import folio_user_import_manager.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            "",
            "",
            "",
            "",
            data_location=request.path.parent / "samples" / "schema_not_ok.csv",
        ),
    )

    assert res.read_ok, str(res.read_errors["data"]) if res.read_errors else None
    assert not res.schema_ok


def test_check_read_not_ok(request: pytest.FixtureRequest) -> None:
    import folio_user_import_manager.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            "",
            "",
            "",
            "",
            data_location=request.path.parent / "samples" / "read_not_ok.csv",
        ),
    )

    assert not res.read_ok
    assert res.schema_ok, str(res.schema_errors["data"]) if res.schema_errors else None
