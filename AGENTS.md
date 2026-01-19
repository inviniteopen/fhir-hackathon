# AGENTS.md

Project-specific instructions for AI assistants.

## Library Versions

This project uses the latest versions of dependencies. Always use current APIs, not deprecated ones.

### Pydantic (v2+)

Use Pydantic v2 APIs:

Documentation: Use Context7 with library ID `/pydantic/pydantic` to query current API usage.

### fhir.resources (v8+)

The `fhir.resources` library is built on Pydantic. Use Pydantic v2 methods for all FHIR model operations.

Documentation: https://github.com/nazrulworld/fhir.resources

## Code Style

- Python 3.11+
- Type hints required
- Use `Path` from `pathlib` for file operations

## DAS (Data as Software)

The `das/` module provides typed dataframe abstractions. Use it when working with parsed FHIR data in dataframes.

### When to use DAS

- Loading FHIR resources into dataframes for analysis
- Performing aggregations, filtering, or transformations on FHIR data
- When type safety for dataframe operations is needed

### Available engines

- **DuckDB** (`das.engine.duckdb`) - Preferred for analytical queries
- **Polars** (`das.engine.polars`) - Alternative high-performance option

### Guidelines

- Prefer DuckDB for most use cases (fast, lightweight, SQL-compatible)
- Use typed relations/dataframes from `das.engine` for type safety
- Do not modify the `das/` source code - it's shared infrastructure
- PySpark engine exists but is disabled for this project (simplicity)
