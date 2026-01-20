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

    from constants import BRONZE_SCHEMA, GOLD_SCHEMA, SILVER_SCHEMA
    from fhir_loader import get_table_summary
    from transformations.patients import get_patient_summary
    from validations.patients import get_validation_report
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
def _(mo):
    obs_bin_width = mo.ui.slider(
        start=1,
        stop=50,
        value=5,
        step=1,
        label="Observation count bin width",
    )
    obs_bin_width
    return (obs_bin_width,)


@app.cell
def _(GOLD_SCHEMA, con, mo, obs_bin_width):
    import plotly.graph_objects as go

    df = con.sql(
        f"""
        SELECT observation_count
        FROM {GOLD_SCHEMA}.observations_per_patient
        """
    ).df()

    bin_w = int(obs_bin_width.value)
    fig = go.Figure(
        data=[
            go.Histogram(
                x=df["observation_count"],
                xbins={"size": bin_w},
                marker={"color": "#4C78A8"},
            )
        ]
    )
    fig.update_layout(
        title="Observations per patient",
        xaxis_title="Observation count",
        yaxis_title="Patients",
        bargap=0.05,
        height=360,
    )

    mo.ui.plotly(fig)
    return (go,)


@app.cell
def _(GOLD_SCHEMA, con, go, mo):
    def _():
        age_df = con.sql(
            f"""
            SELECT
              FLOOR(patient_age_years / 5) * 5 AS age_bin_start,
              COUNT(*) AS patients,
              SUM(observation_count) AS observations
            FROM {GOLD_SCHEMA}.observations_per_patient
            WHERE patient_age_years IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ).df()


        labels = [
            f"{int(a)}-{int(a) + 4}" for a in age_df["age_bin_start"].tolist()
        ]

        patients_fig = go.Figure(
            data=[
                go.Bar(
                    x=labels,
                    y=age_df["patients"],
                    marker={"color": "#F58518"},
                )
            ]
        )
        patients_fig.update_layout(
            title="Patients by age range (5-year bins)",
            xaxis_title="Age range (years)",
            yaxis_title="Patients",
            height=360,
        )

        observations_fig = go.Figure(
            data=[
                go.Bar(
                    x=labels,
                    y=age_df["observations"],
                    marker={"color": "#54A24B"},
                )
            ]
        )
        observations_fig.update_layout(
            title="Total observations by age range (5-year bins)",
            xaxis_title="Age range (years)",
            yaxis_title="Observations",
            height=360,
        )
        return mo.vstack([mo.ui.plotly(patients_fig), mo.ui.plotly(observations_fig)])


    _()
    return


if __name__ == "__main__":
    app.run()
