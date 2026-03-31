# dbt-cost

Estimate BigQuery query costs for your dbt models **before** running them.

`dbt-cost` compiles your models, then calls BigQuery's dry-run API to report how much data each model would scan and what it would cost under on-demand pricing. Nothing is executed, nothing is billed.

```
══════════════════════════════════════════════════════
  💰 dbt-cost: BigQuery Cost Estimation
══════════════════════════════════════════════════════

  ──────────────────────────────────────────────────────────────────────────────────────────
  Model                             Scanned    Est. Cost    Accuracy               Type
  ──────────────────────────────────────────────────────────────────────────────────────────
  - fct_revenue                      48.30 GB     $0.3019   UPPER_BOUND      incremental
  - stg_orders                        1.20 GB     $0.0075      PRECISE            table
  - dim_customers                          —       $0.00            —              view
  ──────────────────────────────────────────────────────────────────────────────────────────
  TOTAL                              49.50 GB      $0.3094
  ──────────────────────────────────────────────────────────────────────────────────────────
```

## How it works

1. Runs `dbt compile` with all the flags you pass in (`-s`, `--target`, `--vars`, `--full-refresh`, etc.).
2. Reads `target/manifest.json` to extract the compiled SQL for each selected model.
3. Sends each query to the BigQuery Jobs API with `dryRun: true`. BigQuery runs the full query planner against table metadata — reflecting partition pruning and giving a conservative upper-bound — without executing anything.
4. Prints a cost report based on on-demand pricing ($6.25/TB by default).

Models materialized as `view` or `ephemeral` are skipped since they don't scan data at build time.

## Install

Requires [uv](https://docs.astral.sh/uv/) and valid BigQuery credentials (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`).

```bash
uv tool install git+https://github.com/merlixo/dbt-cost.git
```

This installs `dbt-cost` as a global CLI command. To update later:

```bash
uv tool upgrade dbt-cost
```

### Alternative install methods

**As a dev dependency in a dbt project's `pyproject.toml`:**

```toml
[dependency-groups]
dev = [
    "dbt-cost @ git+https://github.com/merlixo/dbt-cost.git",
]
```

```bash
uv sync
uv run dbt-cost -s +fct_revenue
```

**Run without installing:**

```bash
uvx --from git+https://github.com/merlixo/dbt-cost.git dbt-cost -s +fct_revenue
```

## Usage

Run from your dbt project directory (where `dbt_project.yml` lives). All arguments are forwarded to `dbt compile`, so any flag that works with `dbt run` works here.

```bash
# All models
dbt-cost

# Single model
dbt-cost -s fct_revenue

# Model and its upstream dependencies
dbt-cost -s +fct_revenue

# By tag
dbt-cost -s tag:finance

# Against a specific target
dbt-cost -s +fct_revenue --target prod

# With dbt variables
dbt-cost -s +fct_revenue --vars '{start_date: "2026-01-01"}'

# Full-refresh cost for an incremental model
dbt-cost --full-refresh -s fct_revenue

# From a different directory
dbt-cost --project-dir ./dbt -s +fct_revenue
```

## How BigQuery dry-run works

The `dryRun: true` flag tells BigQuery to run the full query planner without executing the query.

- **Free.** No slots consumed, no bytes billed.
- **Partition pruning is reflected.** If your query filters on a partitioned column, the estimate covers only the matching partitions.
- **Clustering savings are partially reflected.** The estimate is conservative because block-level pruning can't be predicted without execution.
- **Query cache is ignored.** The estimate always reflects a cold run.
- **Accuracy indicator.** Each estimate includes a `totalBytesProcessedAccuracy` value: `PRECISE` (exact), `UPPER_BOUND` (actual cost will be equal or lower), `LOWER_BOUND`, or `UNKNOWN`.

## Limitations

- **BigQuery only.** On-demand pricing. Does not estimate slot-time for Editions (capacity-based) pricing.
- **Incremental models** are estimated based on their compiled incremental SQL (with the `WHERE` filter). Use `--full-refresh` to estimate a full table rebuild.
- **Upper-bound estimates.** Actual cost may be lower due to clustering and caching.
- **No storage costs.** Only estimates query/scan costs, not storage for the materialized output.
- **Federated queries** (external data sources) may report 0 bytes.

## Requirements

- Python >= 3.10
- `dbt-core` and `dbt-bigquery` installed in your dbt project's environment (not bundled — `dbt-cost` shells out to the `dbt` CLI)
- BigQuery credentials with `bigquery.jobs.create` permission
