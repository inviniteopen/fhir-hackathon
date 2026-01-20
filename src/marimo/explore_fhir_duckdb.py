"""Marimo notebook: explore FHIR resources loaded into DuckDB."""

import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


@app.cell
def _():
    import sys
    from pathlib import Path

    import marimo as mo

    # Add project root (for das) and src (for local modules) to path
    repo_root = Path(__file__).parent.parent.parent
    src_root = repo_root / "src"
    for p in [repo_root, src_root]:
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))

    import duckdb
    import pandas as pd
    import polars as pl

    from constants import BRONZE_SCHEMA, SILVER_SCHEMA
    from fhir_loader import get_table_summary
    from transformations.patients import get_patient_summary
    from validations.patients import get_validation_report
    return (
        BRONZE_SCHEMA,
        SILVER_SCHEMA,
        duckdb,
        get_patient_summary,
        get_table_summary,
        get_validation_report,
        mo,
        pd,
        pl,
        repo_root,
    )


@app.cell
def _(mo):
    mo.md("""
    # FHIR DuckDB exploration

    This notebook connects to a DuckDB database created by `main.py` and explores
    both bronze (raw) and silver (transformed) FHIR data layers.
    """)
    return


@app.cell
def _(mo, repo_root):
    default_db_path = repo_root / "fhir.duckdb"
    db_path = mo.ui.text(
        value=str(default_db_path),
        label="DuckDB database path",
        full_width=True,
    )
    db_path
    return (db_path,)


@app.cell
def _(db_path, duckdb):
    con = duckdb.connect(db_path.value, read_only=True)
    return (con,)


@app.cell
def _(mo):
    mo.md("""
    ## Bronze Layer Summary
    """)
    return


@app.cell
def _(con, get_table_summary, pd):
    summary = get_table_summary(con)
    summary_df = (
        pd.DataFrame(
            [{"table": name, "rows": rows} for name, rows in summary.items()]
        )
        .sort_values(["rows", "table"], ascending=[False, True])
        .reset_index(drop=True)
    )
    summary_df
    return


@app.cell
def _(BRONZE_SCHEMA, con, mo):
    bronze_table_names = [
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            [BRONZE_SCHEMA],
        ).fetchall()
    ]
    table = mo.ui.dropdown(
        options=bronze_table_names,
        value=bronze_table_names[0] if bronze_table_names else None,
        label="Table to sample",
    )
    table
    return (table,)


@app.cell
def _(BRONZE_SCHEMA, con, mo, table):
    sample_df = con.sql(
        f'SELECT * FROM {BRONZE_SCHEMA}."{table.value}" LIMIT 10'
    ).df()
    mo.ui.table(sample_df, selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Silver Layer
    """)
    return


@app.cell
def _(SILVER_SCHEMA, con, mo):
    silver_table_names = [
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            [SILVER_SCHEMA],
        ).fetchall()
    ]
    silver_table = mo.ui.dropdown(
        options=silver_table_names,
        value=silver_table_names[0] if silver_table_names else None,
        label="Table to sample",
    )
    silver_table
    return (silver_table,)


@app.cell
def _(SILVER_SCHEMA, con, mo, silver_table):
    silver_sample_df = con.sql(
        f'SELECT * FROM {SILVER_SCHEMA}."{silver_table.value}" LIMIT 10'
    ).df()
    mo.ui.table(silver_sample_df, selection=None)
    return


if __name__ == "__main__":
    app.run()
