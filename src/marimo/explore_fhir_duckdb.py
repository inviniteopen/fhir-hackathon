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
    table_names = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    options = table_names if table_names else ["<no tables>"]
    table = mo.ui.dropdown(
        options=options,
        value=options[0],
        label="Table to sample",
    )
    table
    return (table,)


@app.cell
def _(con, pd, table):
    sample_df = (
        pd.DataFrame()
        if table.value.startswith("<")
        else con.sql(f"SELECT * FROM {table.value} LIMIT 25").df()
    )
    sample_df
    return


@app.cell
def _(mo):
    sql = mo.ui.text_area(
        value="SELECT * FROM patient LIMIT 10",
        label="Ad-hoc SQL",
        full_width=True,
    )
    sql
    return (sql,)


@app.cell
def _(con, pd, sql):
    try:
        df = con.sql(sql.value).df()
    except Exception as exc:  # noqa: BLE001 - notebook UX: show errors inline
        df = pd.DataFrame({"error": [str(exc)]})

    df
    return


if __name__ == "__main__":
    app.run()
