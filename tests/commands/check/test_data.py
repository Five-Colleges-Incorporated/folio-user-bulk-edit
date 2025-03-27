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
