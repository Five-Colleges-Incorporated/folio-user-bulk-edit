import typing
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from pytest_cases import parametrize, parametrize_with_cases

_samples = list(
    (Path() / "tests" / "commands" / "user_import" / "samples").glob("*.csv"),
)


@dataclass
class TransformationTestCase:
    data_location: dict[str, Path]
    expected: dict[str, typing.Any]


class TransformationCases:
    @parametrize(csv=_samples)
    def case_ok(self, csv: Path) -> TransformationTestCase:
        return TransformationTestCase({"data": csv}, {})


@mock.patch("pyfolioclient.FolioBaseClient")
@parametrize_with_cases("tc", TransformationCases)
def test_check_data(
    base_client_mock: mock.Mock,
    tc: TransformationTestCase,
) -> None:
    import folio_user_import_manager.commands.user_import as uut

    # I couldn't figure this out better
    post_data_mock = base_client_mock().__enter__().post_data

    uut.run(
        uut.ImportOptions(
            "",
            "",
            "",
            "",
            tc.data_location,
            10000,
            1,
            0,
            100.0,
            deactivate_missing_users=False,
            update_all_fields=False,
            source_type=None,
        ),
    )

    post_data_mock.assert_called_with("/user-import", payload=tc.expected)
