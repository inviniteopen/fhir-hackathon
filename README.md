# FHIR Bundle Parser Demo

This demo loads FHIR Bundle JSON files from `data/EPS` into DuckDB tables (one
table per `resourceType`) for analysis.

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

## Read the data (DuckDB)

This project materializes each FHIR `resourceType` into a DuckDB table (one table
per resource type) under the `bronze` schema (e.g. `bronze.patient`,
`bronze.observation`, `bronze.condition`).

Create/update the DuckDB database (defaults to `fhir.duckdb`):

```bash
uv run main.py
```

Use a different input directory or database path:

```bash
uv run main.py data/EPS --db /tmp/fhir.duckdb
```

## Explore with Marimo

The exploration notebook lives at:

`src/marimo/explore_fhir_duckdb.py`

Run the notebook editor:

```bash
uv run --group notebook marimo edit src/marimo/explore_fhir_duckdb.py
```

The notebook defaults to opening `fhir.duckdb` at the repo root, but you can
change the path from within the notebook UI.

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
- Extracts each `entry.resource` and groups rows by `resourceType`.
- Creates one DuckDB table per `resourceType` (lowercased).
- Tables live in the `bronze` schema (medallion architecture: bronze layer).
- Adds provenance columns: `_source_file`, `_source_bundle`, `_full_url`.

The summary reports how many resource tables were created and the row counts.

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

### Improving test data quality with Data-as-Software

One of the main benefits of the Data-as-Software (DAS) paradigm is higher-quality test data. When test data curation, normalization, and validation are expressed as code, you can apply the same rigor that software teams already use for correctness and maintainability.

Key advantages:

- **Runs anywhere**: the logic is just Python, so the same test-data pipeline can run in public cloud, private cloud, or on-prem (including developer laptops and CI).
- **First-class testing**: use `pytest` to unit test transformations, validate invariants, and lock in expected behavior as the data and schemas evolve.
- **Modern developer tooling**: linters, formatters, type checkers, and language servers (autocomplete, go-to-definition, refactors) work out of the box, making it easier to evolve data logic safely.
- **Versioned, reviewable changes**: treat test datasets and their generators like software artifacts—code review, diffs, and reproducible builds reduce “mysterious” changes.
- **Composable and reusable**: small, well-tested transformation functions can be reused across pipelines and test suites instead of being re-implemented in multiple places.

This complements (rather than replaces) the traditional SQL-centric approach. SQL is excellent for expressing set-based analytics and is widely understood, but data workflows implemented primarily as SQL can face challenges like:

- **Logic spread across many queries** (and sometimes multiple systems), which can make it harder to modularize and reuse.
- **Testing and refactoring friction**, especially when “business rules” are encoded in long queries without the same unit-testing ergonomics.
- **Limited IDE-aware tooling** compared to general-purpose programming languages (static typing, richer refactors, tighter integration with test runners).

With DAS, you can still use SQL where it shines (for example, via DuckDB), but put the workflow under software engineering discipline: typed models (`fhir.resources`), explicit validation, and a test suite that continuously guards test data quality.

### Running DAS tests

```bash
uv run pytest tests/duckdb/ tests/polars/
```
