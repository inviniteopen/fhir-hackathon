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
