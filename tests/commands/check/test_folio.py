from pytest_cases import parametrize, parametrize_with_cases

_okapi = "https://folio-snapshot-okapi.dev.folio.org"
_eureka = "https://folio-etesting-snapshot-kong.ci.folio.org"
_tenant = "diku"
_username = "diku_admin"
_password = "admin"  # noqa:S105


class FolioErrorCases:
    @parametrize(reason=("https://bad.url", _okapi.split("/")[-1]))
    def case_url(
        self,
        reason: str,
    ) -> tuple[str, ...]:
        return (reason, _tenant, _username, _password, "Invalid FOLIO Url")

    @parametrize(env=(_okapi, _eureka))
    def case_services_url(
        self,
        env: str,
    ) -> tuple[str, ...]:
        return (
            env.replace("-okapi", "").replace("-kong", "-diku"),
            _tenant,
            _username,
            _password,
            "Could Not Login",
        )

    @parametrize(env=(_okapi, _eureka))
    def case_tenant(
        self,
        env: str,
    ) -> tuple[str, ...]:
        return (
            env,
            "bad-tenant",
            _username,
            _password,
            "Invalid Tenant",
        )

    @parametrize(env=(_okapi, _eureka))
    def case_username(
        self,
        env: str,
    ) -> tuple[str, ...]:
        return (
            env,
            _tenant,
            "bad-username",
            _password,
            "Could Not Login",
        )

    @parametrize(env=(_okapi, _eureka))
    def case_password(
        self,
        env: str,
    ) -> tuple[str, ...]:
        return (
            env,
            _tenant,
            _username,
            "bad-password",
            "Could Not Login",
        )


@parametrize(folio_url=(_okapi, _eureka))
def test_check_folio_ok(folio_url: str) -> None:
    import folio_user_bulk_edit.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            folio_url,
            folio_tenant="diku",
            folio_username="diku_admin",
            folio_password="admin",  # noqa:S106
            data_location={},
        ),
    )
    assert res.folio_ok, res.folio_error


@parametrize_with_cases(
    "folio_url, folio_tenant, folio_username, folio_password, expected_error",
    cases=FolioErrorCases,
)
def test_check_folio_error(
    folio_url: str,
    folio_tenant: str,
    folio_username: str,
    folio_password: str,
    expected_error: str,
) -> None:
    import folio_user_bulk_edit.commands.check as uut

    res = uut.run(
        uut.CheckOptions(
            folio_url,
            folio_tenant=folio_tenant,
            folio_username=folio_username,
            folio_password=folio_password,
            data_location={},
        ),
    )
    assert not res.folio_ok
    assert res.folio_error == expected_error
