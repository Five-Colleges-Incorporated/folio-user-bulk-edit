def test_check_folio_ok() -> None:
    import folio_user_import_manager.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            "folio-snapshot-okapi.dev.folio.org",
            "diku",
            "diku_admin",
            "admin",
        ),
    )
    assert res.folio_ok, res.folio_error
