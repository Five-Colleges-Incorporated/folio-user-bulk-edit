# FOLIO User Bulk Edit

Initiates, monitors, and reports on bulk user operations in FOLIO.
Not to be confused with the very limited [user capabilities of bulk edit](https://docs.folio.org/docs/bulk-edit/#users-1).

## Installation

User Bulk Edit is [available on PyPi](https://pypi.org/project/folio-user-bulk-edit/). It can be installed using:
```sh
pip install folio-user-bulk-edit
```

Additionally if you wish to only use the cli you can install it using pipx:
```sh
pipx install folio-user-bulk-edit
```

Check out the [CHANGELOG](./CHANGELOG.md) for what's new!

## Usage

### As a cli tool

The executable to run the tool is `ube`.
If you installed using pip or pipx it should be on your path and executable.

You can also run the cli without installing using pipx:
```sh
pipx run --spec folio-user-bulk-edit ube
```

By default a `./logs` directory will be created to store more verbose logging information that is output to the console.
This location is controlled by using the `--log-directory` parameter.

Many parameters can be specified using environment variables.
The general format is `UBE__SECTION_NAME__VARIABLE_NAME`.
Run `ube --help` for a full list of options and the corresponding environment variables.



The User Bulk Edit has two modes which take the same core parameters.

#### `ube check <data>`

This command will ensure the connection to FOLIO is ok and report any issues with the data.
Check makes no changes to FOLIO or your local file system and is ok to run repeatedly.
It is using the Pandera library, [consult the documentation](https://pandera.readthedocs.io/en/stable/index.html#informative-errors) for more information on interpreting the check.

Check is a "best effort" check.
Data with check errors may still import into FOLIO fine, and data without check errors may still encounter issues during import.


#### `ube import <data>`

This command will import the user data file through the [mod-user-import](https://github.com/folio-org/mod-user-import) endpoint.
The data will be batched and as much of it will be imported as possible.
The user-mod-import endpoint returns information about users imported, this information is summarized and reported after running.
The full list of errored users and causes can be found in the log directory as a csv.


### As a library

If you are programatically calling ube you can use it as a python library by doing the following:

```python
from pathlib import Path
from folio_bulk_user_edit.commands import check, user_import

check_results = check.run(check.CheckOptions(
    folio_url="...",
    folio_tenant="...",
    folio_username="...",
    folio_password="...",
    data_location=Path("to" / "file.csv"),
))

if check_results.folio_ok and check_results.schema_ok and check_results.read_ok:
    import_results = user_import.run(check.ImportOptions(
        folio_url="...",
        folio_tenant="...",
        folio_username="...",
        folio_password="...",
        data_location=Path("to" / "file.csv"),
        batch_size=1000,
        retry_count=1,
        deactivate_missing_users=False,
        update_all_fields=False,
    ))
```

There are some utility subpackages in the root package that might also be useful.
Especially the schemas.

## Contributing

See [CONTRIBUTING](./CONTRIBUTING.md).
