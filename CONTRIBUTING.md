# Contributing to FOLIO User Bulk Edit

Contributions to this project are welcome as long as it remains maintainable over the long term.

## Setup

This project uses pdm.
You can find the [installation instructions on pdm's website](https://pdm-project.org/en/latest/#installation).

To install library dependencies and developer tools run:
```sh
pdm install -G:all
```

It is recommended that you use an IDE with ruff, mypy, and pytest integration.

## Typing, linting, formatting, and code standards

This project is using ruff for linting and formatting.
You can find the ruff configuration in the pyproject.toml file in the root of the repository.
It is _very_ strict with almost every rule enabled.
When in doubt, **ruff is correct** and the code should be changed to make ruff happy.

This project is using mypy for type checking.
You can find the mypy configuration in the pyproject.toml file in the root of the repository.
It is _very_ strict with every rule enabled.
When in doubt, **mypy is correct** and the code should be changed to make mypy happy.
This means type-hinting everything!

If there _really_ is no alternative mypy and ruff can be disabled in a line-by-line, rule-by-rule basis.
If you feel stuck using ruff and mypy feel free to open a PR to get some help.

You may also want to [install precious](https://github.com/houseabsolute/precious/blob/master/README.md#installation), the One Code Quality Tool to Rule Them All.  
Running `precious lint` or `precious tidy` will pickup the precious.toml file in the root of the repository.
The CI lint job is setup in this same way.

## Tests

You can run all the tests in the project using
```sh
pdm run test
# pytest parameters are also ok
pdm run test -k test_schema -s
```

[Table-driven tests](https://go.dev/wiki/TableDrivenTests) using [pytest-cases](https://smarie.github.io/python-pytest-cases/) are preferred.

No code will be merged without
1. Existing tests passing
1. New tests added to cover new functionality

If you feel stuck testing your code feel free to open a PR to get some help.
