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
def _(SILVER_SCHEMA, con, mo, pd):
    # Get silver tables
    silver_tables = [
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

    if silver_tables:
        silver_summary = []
        for tbl in silver_tables:
            count = con.execute(
                f'SELECT COUNT(*) FROM {SILVER_SCHEMA}."{tbl}"'
            ).fetchone()[0]
            silver_summary.append({"table": f"{SILVER_SCHEMA}.{tbl}", "rows": count})
        silver_summary_df = pd.DataFrame(silver_summary)
        mo.ui.table(silver_summary_df, selection=None)
    else:
        mo.md("_No silver tables found. Run `main.py` to create silver layer._")
    return


@app.cell
def _(SILVER_SCHEMA, con, get_patient_summary, mo, pd, pl):
    # Patient data quality
    try:
        silver_patient_lf = pl.from_arrow(
            con.execute(f"SELECT * FROM {SILVER_SCHEMA}.patient").arrow()
        ).lazy()
        patient_summary = get_patient_summary(silver_patient_lf)
        total = patient_summary["total_patients"]

        quality_data = [
            {
                "field": k.replace("with_", ""),
                "count": v,
                "percentage": f"{v / total * 100:.0f}%",
            }
            for k, v in patient_summary.items()
            if k != "total_patients"
        ]
        quality_df = pd.DataFrame(quality_data)

        mo.vstack(
            [mo.md("### Patient Data Quality"), mo.ui.table(quality_df, selection=None)]
        )
    except Exception:
        mo.md("_Silver patient table not available._")
    return


@app.cell
def _(SILVER_SCHEMA, con, get_validation_report, mo, pd, pl):
    # Validation results
    try:
        silver_patient_lf_val = pl.from_arrow(
            con.execute(f"SELECT * FROM {SILVER_SCHEMA}.patient").arrow()
        ).lazy()
        validation_report = get_validation_report(silver_patient_lf_val)

        validation_summary = pd.DataFrame(
            [
                {
                    "total_records": validation_report["total_records"],
                    "valid_records": validation_report["valid_records"],
                    "invalid_records": validation_report["invalid_records"],
                    "validity_rate": f"{validation_report['validity_rate']:.1%}",
                }
            ]
        )

        elements = [
            mo.md("### Validation Results"),
            mo.ui.table(validation_summary, selection=None),
        ]

        if validation_report["errors_by_rule"]:
            errors_df = pd.DataFrame(validation_report["errors_by_rule"])
            elements.append(mo.md("#### Errors by Rule"))
            elements.append(mo.ui.table(errors_df, selection=None))

        mo.vstack(elements)
    except Exception:
        mo.md("_Validation data not available._")
    return


@app.cell
def _(SILVER_SCHEMA, con, mo):
    # Silver table browser
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

    if silver_table_names:
        silver_table = mo.ui.dropdown(
            options=silver_table_names,
            value=silver_table_names[0],
            label="Silver table to sample",
        )
        silver_limit = mo.ui.slider(
            start=1,
            stop=200,
            value=25,
            step=1,
            label="Rows",
        )
        mo.hstack([silver_table, silver_limit], justify="space-between", align="center")
    else:
        silver_table = None
        silver_limit = None
        mo.md("_No silver tables available._")
    return silver_limit, silver_table


@app.cell
def _(SILVER_SCHEMA, con, mo, silver_limit, silver_table):
    if silver_table is not None:
        silver_sample_df = con.sql(
            f'SELECT * FROM {SILVER_SCHEMA}."{silver_table.value}" LIMIT {int(silver_limit.value)}'
        ).df()
        mo.ui.table(silver_sample_df, selection=None)
    return


if __name__ == "__main__":
    app.run()
