"""Marimo notebook: explore FHIR resources loaded into DuckDB."""

import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


@app.cell
def _():
    import duckdb
    import pandas as pd
    from pathlib import Path

    import marimo as mo

    from fhir_loader import get_table_summary
    return duckdb, get_table_summary, mo, pd


@app.cell
def _(mo):
    mo.md("""
    # FHIR DuckDB exploration

    This notebook connects to a DuckDB database created by `main.py` and runs a few
    lightweight exploratory `SELECT` queries to get a taste of the loaded FHIR data.
    """)
    return


@app.cell
def _(mo):
    repo_root = mo.notebook_dir().parents[1]
    default_db_path = repo_root / "fhir.duckdb"
    return (default_db_path,)


@app.cell
def _(default_db_path, mo):
    db_path = mo.ui.text(
        value=str(default_db_path),
        label="DuckDB database path",
        full_width=True,
    )
    db_path
    return (db_path,)


@app.cell
def _(db_path, duckdb):
    con = duckdb.connect(db_path.value)
    return (con,)


@app.cell
def _(con, get_table_summary, pd):
    summary = get_table_summary(con)
    if summary:
        summary_df = (
            pd.DataFrame(
                [{"table": name, "rows": rows} for name, rows in summary.items()]
            )
            .sort_values(["rows", "table"], ascending=[False, True])
            .reset_index(drop=True)
        )
    else:
        summary_df = pd.DataFrame(columns=["table", "rows"])

    summary_df
    return


@app.cell
def _(con, mo):
    BRONZE_SCHEMA = "bronze"
    table_names = [
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
    options = table_names 
    table = mo.ui.dropdown(
        options=options,
        value=options[0],
        label="Table to sample",
    )
    limit = mo.ui.slider(
        start=1,
        stop=200,
        value=25,
        step=1,
        label="Rows",
    )

    mo.hstack([table, limit], justify="space-between", align="center")
    return BRONZE_SCHEMA, limit, table


@app.cell
def _(BRONZE_SCHEMA, con, limit, mo, table):
    sample_df = con.sql(
            f'SELECT * FROM {BRONZE_SCHEMA}."{table.value}" LIMIT {int(limit.value)}'
        ).df()
    mo.ui.table(sample_df, selection=None)
    return


if __name__ == "__main__":
    app.run()
