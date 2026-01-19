# FHIR Bundle Parser Demo

This demo parses FHIR Bundle JSON files from `data/EPS` into typed
`fhir.resources` models.

## Requirements

- Use `uv` for environment and dependencies.

## Quick start

```bash
uv run main.py
```

By default, the parser reads from:

```
data/EPS
```

To parse a different directory:

```bash
uv run main.py /path/to/bundles
```

To see validation errors for entries that fail to parse:

```bash
uv run main.py --show-errors
```

## Tests

```bash
uv run pytest tests/
```

## Code quality

Format:

```bash
uvx ruff format .
```

Lint:

```bash
uvx ruff check .
```

Type check:

```bash
uvx ty check .
```

## What it does

- Loads each `*.json` file as a Bundle.
- Parses Bundle metadata into a typed `Bundle` model.
- Parses each `entry.resource` into the appropriate typed FHIR model using
  `fhir.resources.get_fhir_model_class`.
- Collects validation errors instead of failing the entire run.

The summary reports how many entries were successfully typed vs. how many
failed validation.

## DAS (Data as Software)

The `das/` module provides typed dataframe abstractions for working with parsed FHIR data. It enables type-safe dataframe operations across multiple engines.

### Supported engines

- **DuckDB** - Fast analytical queries on local data
- **Polars** - High-performance dataframe library

### Purpose

DAS allows you to:

- Load parsed FHIR resources into typed dataframes
- Perform type-safe transformations and analysis
- Use consistent APIs across different dataframe engines

### Running DAS tests

```bash
uv run pytest tests/duckdb/ tests/polars/
```
