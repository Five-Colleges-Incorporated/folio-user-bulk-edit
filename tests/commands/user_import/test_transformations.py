import json
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
        res = json.loads((csv.parent / (csv.stem + ".json")).read_text())
        return TransformationTestCase({"data": csv}, res)


@mock.patch("pyfolioclient.FolioBaseClient")
@parametrize_with_cases("tc", TransformationCases)
def test_transform_data(
    base_client_mock: mock.Mock,
    tc: TransformationTestCase,
) -> None:
    import folio_user_bulk_edit.commands.user_import as uut

    # I couldn't figure this out better
    post_data_mock: mock.MagicMock = (
        base_client_mock.return_value.__enter__.return_value.post_data
    )

    uut.run(
        uut.ImportOptions(
            "",
            "",
            "",
            "",
            tc.data_location,
            10000,
            0,
            deactivate_missing_users=False,
            update_all_fields=False,
            source_type=None,
        ),
    )

    post_data_mock.assert_called_with("/user-import", payload=tc.expected)
