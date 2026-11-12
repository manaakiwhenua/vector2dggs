# Contributing to vector2dggs

Contributions are welcome: bug reports, questions, documentation fixes, and code.

## Reporting a problem

Open an issue at <https://github.com/manaakiwhenua/vector2dggs/issues>. A report is much easier to act on if it includes:

- the `vector2dggs` version (`vector2dggs --version`), Python version and operating system;
- the full command you ran;
- the full error output, or a description of what was wrong with the result;
- the output of `ogrinfo -so` for the input layer or, better, a small file that reproduces the problem.

## Asking a question or getting help

Use the same issue tracker. Questions about usage, about which containment mode suits your data, and requests for new grids or options are all fine there.

## Contributing code

For anything larger than a small fix, please open an issue first so the approach can be agreed before you spend time on it.

1. Fork the repository and create a branch from `main`.
2. Set up a development environment as described under [For development](README.md#for-development) in the README. The tests need every backend and the development tools: `poetry install -E all --with dev`. No system GDAL is required.
3. Make your change, with tests.
4. Run the checks that continuous integration runs:

   ```bash
   ruff check .
   black --check .
   mypy vector2dggs/
   python -m pytest -n auto tests/
   ```

   Tests of the PostgreSQL/PostGIS input path start a throwaway PostGIS container and are skipped automatically if Docker is not available.

5. Open a pull request against `main` describing what changed and why.

### Adding a grid

A new DGGS backend needs an indexer class that subclasses `VectorIndexer` (`vector2dggs/indexers/vectorindexer.py`), an entry in `INDEXER_LOOKUP` (`vector2dggs/indexerfactory.py`), a command module built with `make_dggs_command` (see `vector2dggs/h3.py`) and registered in `vector2dggs/cli.py`, and an optional-dependency extra in `pyproject.toml`.

## Licence

vector2dggs is licensed under LGPL-3.0-or-later. By contributing you agree that your contribution is licensed under the same terms.
