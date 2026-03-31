#!/usr/bin/env python3
"""
dbt-cost: BigQuery cost estimation for dbt models.

Compiles selected dbt models, then uses BigQuery's dry-run API
to estimate the bytes processed and on-demand cost for each model.

Usage:
    dbt-cost                                    # all models
    dbt-cost -s fct_revenue                     # single model
    dbt-cost --full-refresh -s fct_revenue      # full-refresh estimate
"""

import json
import sys
from pathlib import Path
from dataclasses import dataclass
import subprocess

PRICE_PER_TB = 6.25

# ──────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────

@dataclass
class CostEstimate:
    model_name: str
    materialization: str
    bytes_processed: int
    accuracy: str
    skipped: bool = False
    skip_reason: str = ""
    error: str = ""

    @property
    def gb(self) -> float:
        return self.bytes_processed / (1024**3)

    @property
    def tb(self) -> float:
        return self.bytes_processed / (1024**4)

    @property
    def cost_usd(self) -> float:
        return self.tb * PRICE_PER_TB


@dataclass
class CompiledModel:
    unique_id: str
    name: str
    materialization: str
    compiled_code: str
    full_refresh: bool | None = None


# ──────────────────────────────────────────────
# Step 1: Compile models via dbt
# ──────────────────────────────────────────────

def run_dbt_compile(dbt_args: list[str]) -> None:
    """Run dbt compile, forwarding all arguments as-is."""
    cmd = ["dbt", "compile"] + dbt_args
    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: dbt compile failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)


# ──────────────────────────────────────────────
# Step 2: Read manifest and extract compiled SQL
# ──────────────────────────────────────────────

def read_manifest() -> list[CompiledModel]:
    """Parse manifest.json and extract compiled SQL for all models."""
    manifest_path = Path(".") / "target" / "manifest.json"

    if not manifest_path.exists():
        print(f"  ERROR: {manifest_path} not found. dbt compile may have failed.", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    models = []
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        compiled_code = node.get("compiled_code") or node.get("compiled_sql", "")
        if not compiled_code:
            continue

        config = node.get("config", {})
        materialization = config.get("materialized", "unknown")
        full_refresh = config.get("full_refresh", None)

        models.append(CompiledModel(
            unique_id=unique_id,
            name=node["name"],
            materialization=materialization,
            compiled_code=compiled_code,
            full_refresh=full_refresh,
        ))

    return models


# ──────────────────────────────────────────────
# Step 3: Dry-run each model against BigQuery
# ──────────────────────────────────────────────

def estimate_model_cost(client, model: CompiledModel) -> CostEstimate:
    from google.cloud import bigquery

    if model.materialization in  {"view", "ephemeral"} or (model.materialization == "materialized_view" and model.full_refresh is False):
        return CostEstimate(
            model_name=model.name,
            materialization=model.materialization,
            bytes_processed=0,
            accuracy="N/A",
            skipped=True,
            skip_reason=f"{model.materialization}",
        )

    try:
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )

        query_job = client.query(model.compiled_code, job_config=job_config)

        accuracy = "UNKNOWN"
        stats = getattr(query_job, "_properties", {}).get("statistics", {}).get("query", {})
        accuracy = stats.get("totalBytesProcessedAccuracy", accuracy)

        return CostEstimate(
            model_name=model.name,
            materialization=model.materialization,
            bytes_processed=query_job.total_bytes_processed,
            accuracy=accuracy,
        )

    except Exception as e:
        return CostEstimate(
            model_name=model.name,
            materialization=model.materialization,
            bytes_processed=0,
            accuracy="ERROR",
            error=str(e),
        )


# ──────────────────────────────────────────────
# Step 4: Report
# ──────────────────────────────────────────────

def print_report(estimates: list[CostEstimate]) -> float:
    name_w = max(30, max((len(e.model_name) for e in estimates), default=30) + 2)

    header = (
        f"  {'Model':<{name_w + 2}}"
        f"  {'Scanned':>12}"
        f"  {'Est. Cost':>12}"
        f"  {'Accuracy':>12}"
        f"  {'Type':>17}"
    )
    separator = "  " + "─" * (name_w + 12 + 12 + 12 + 27)

    print(separator)
    print(header)
    print(separator)

    total_bytes = 0
    total_cost = 0.0
    errors = []

    for e in sorted(estimates, key=lambda x: x.cost_usd, reverse=True):
        if e.error:
            errors.append(e)
            print(f"  ❌ {e.model_name:<{name_w}}  {'ERROR':>12}  {'—':>12}  {'—':>12}  {e.materialization:>17}")
        elif e.skipped:
            print(f"  - {e.model_name:<{name_w}}  {'—':>12}  {'$0.00':>12}  {'—':>12}  {e.skip_reason:>17}")
        else:
            total_bytes += e.bytes_processed
            total_cost += e.cost_usd
            gb_str = f"{e.gb:.2f} GB"
            cost_str = f"${e.cost_usd:.4f}"
            print(
                f"  - {e.model_name:<{name_w}}"
                f"  {gb_str:>12}"
                f"  {cost_str:>12}"
                f"  {e.accuracy:>12}"
                f"  {e.materialization:>17}"
            )

    print(separator)
    total_gb = total_bytes / (1024**3)
    print(
        f"  {'TOTAL':<{name_w + 2}}"
        f"  {total_gb:>9.2f} GB"
        f"  {'$' + f'{total_cost:.4f}':>12}"
    )
    print(separator)

    if errors:
        print("\n  ⚠️  Errors:")
        for e in errors:
            print(f"     {e.model_name}: {e.error}")

    return total_cost


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    dbt_args = sys.argv[1:]

    run_dbt_compile(dbt_args)

    models = read_manifest()

    if not models:
        print("  No compiled models found.")
        sys.exit(0)

    from google.cloud import bigquery
    client = bigquery.Client()

    estimates = []
    for model in models:
        estimate = estimate_model_cost(client, model)
        estimates.append(estimate)

    total_cost = print_report(estimates)


if __name__ == "__main__":
    main()
