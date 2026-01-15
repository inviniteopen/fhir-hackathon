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
