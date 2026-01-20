"""Marimo notebook: explore FHIR resources loaded into DuckDB."""

import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


@app.cell
def _():
    import sys
    from pathlib import Path

    import marimo as mo

    # Add project root (for `das/` and the `src` package) to path
    repo_root = Path(__file__).parent.parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    import duckdb
    import pandas as pd
    import polars as pl

    from src.bronze.loader import get_table_summary
    from src.constants import BRONZE_SCHEMA, GOLD_SCHEMA, SILVER_SCHEMA
    from src.silver.s1 import get_patient_summary
    from src.validations.patients import get_validation_report
    return (
        BRONZE_SCHEMA,
        GOLD_SCHEMA,
        SILVER_SCHEMA,
        duckdb,
        get_table_summary,
        mo,
        pd,
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
def _(duckdb):
    def get_table_names(
        con: duckdb.DuckDBPyConnection, schema: str
    ) -> list[str]:
        return [
            row[0]
            for row in con.sql(
                """
                SELECT table_name
                FROM duckdb_tables()
                WHERE schema_name = ?
                  AND internal = FALSE
                  AND temporary = FALSE
                ORDER BY table_name
                """,
                params=[schema],
            ).fetchall()
        ]
    return (get_table_names,)


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
def _(BRONZE_SCHEMA, con, get_table_names, mo):
    bronze_table_names = get_table_names(con, BRONZE_SCHEMA)
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
def _(SILVER_SCHEMA, con, get_table_names, mo):
    silver_table_names = get_table_names(con, SILVER_SCHEMA)
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


@app.cell
def _(mo):
    mo.md("""
    ## Gold Layer
    """)
    return


@app.cell
def _(GOLD_SCHEMA, con, get_table_names, mo):
    gold_table_names = get_table_names(con, GOLD_SCHEMA)
    gold_table = mo.ui.dropdown(
        options=gold_table_names,
        value=gold_table_names[0] if gold_table_names else None,
        label="Table to sample",
    )
    gold_table
    return (gold_table,)


@app.cell
def _(GOLD_SCHEMA, con, gold_table, mo):
    gold_sample_df = con.sql(
        f'SELECT * FROM {GOLD_SCHEMA}."{gold_table.value}" LIMIT 10'
    ).df()
    mo.ui.table(gold_sample_df, selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Gold Visualizations

    Uses `gold.observations_per_patient`.
    """)
    return


@app.cell
def _(GOLD_SCHEMA, con, mo):
    import plotly.graph_objects as go

    df = con.sql(
        f"""
        SELECT patient_id, observation_count
        FROM {GOLD_SCHEMA}.observations_per_patient
        ORDER BY observation_count DESC
        """
    ).df()

    fig = go.Figure(
        data=[
            go.Bar(
                y=df["observation_count"]
            )
        ]
    )
    fig.update_layout(
        title="Observations per patient",
        xaxis_title="Patient",
        yaxis_title="Observation count",
        bargap=0.05,
        height=360,
    )

    mo.ui.plotly(fig)
    return (df,)


@app.cell
def _(df):
    observation_counts = df["observation_count"]
    first_20_percent = len(observation_counts) // 5

    top_20_percet_observations = sum(observation_counts[:first_20_percent])
    last_80_percent_observations = sum(observation_counts[first_20_percent:])

    # Tests if most of the observations are caused by the 20 % most active patients
    assert top_20_percet_observations > last_80_percent_observations, f"{top_20_percet_observations} > {last_80_percent_observations} not true"
    return


if __name__ == "__main__":
    app.run()
