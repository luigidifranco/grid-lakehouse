# Design decisions

Each entry: the decision, why, what was traded off, and what would change at a larger scale.

## 1. Build locally first, deploy to Fabric at the end

- **Decision:** Python, Delta Lake, DuckDB and dbt on a laptop; a short pay-as-you-go Fabric F2 deployment at the end.
- **Why:** the Fabric trial expired and a running capacity costs money every hour. Delta tables are the same
  format Fabric's Lakehouse reads, so the move is a copy, not a rewrite.
- **Trade-off:** no scheduler, no managed security, no shared workspace while developing.
- **At scale:** develop against a dev workspace, deploy with CI, pause capacity outside working hours.

## 2. DuckDB instead of Spark

- **Decision:** dbt-duckdb reads the silver Delta tables directly; gold is one DuckDB file.
- **Why:** one year of quarter-hourly data for a handful of zones and borders is a few gigabytes. DuckDB runs
  it in seconds on one machine, at no cost.
- **Trade-off:** single machine, single writer.
- **At scale:** the same dbt models move to Spark (Databricks or Fabric) when the data stops fitting one
  machine or several writers need the tables at once. The medallion design does not change.

## 3. Bronze stores the raw response per market day

- **Decision:** one file per source, dataset and **Brussels market day**, stored untouched, plus a JSONL load log.
- **Why:** a market day in UTC runs 22:00–22:00 in summer and 23:00–23:00 in winter. Splitting by UTC day would
  cut a market day across two files. Keeping the raw response lets silver be rebuilt at any time.
- **Trade-off:** more files and more requests than bulk downloads (ENTSO-E history is fetched month by month).

## 4. A 7-day revision window

- **Decision:** every daily run downloads the last 7 market days again, even if they are already there.
- **Why:** ENTSO-E and Elia correct recent data. Two days downloaded the next morning did not reconcile;
  downloaded again a week later, they matched 100 %.
- **Trade-off:** extra requests on every daily run. Gold marks those days as `provisional`.

## 5. Idempotent MERGE in silver

- **Decision:** silver writes with `MERGE` on the natural key of each table.
- **Why:** reruns and the revision window must never duplicate rows. Re-running silver inserts nothing new.
- **Detail:** no key column may be null, because in a MERGE `null = null` is false and the row would be
  inserted again. JAO base cases get `contingency_name = ''` for that reason.

## 6. Everything in UTC, with the market day alongside

- **Decision:** every timestamp is stored in UTC; each row also carries its Brussels market day.
- **Why:** clock-change days have 92 or 100 quarter-hours. UTC keeps the arithmetic simple; the market day keeps
  the business view. Local wall-clock columns are added only in the serving layer, for hour-of-day charts.

## 7. Warning tests versus blocking tests

- **Decision:** the cross-source discrepancy test warns; the missing-data tests fail the build.
- **Why:** a known, documented difference between two publishers (the Dutch border) should not stop the
  pipeline. A missing day would silently bias every monthly figure, so it must.

## 8. The semantic model as code

- **Decision:** relationships and DAX measures live in a TMDL script in the repository.
- **Why:** reviewable in a merge request, reproducible in a new workspace, and not lost in clicks.

## Known limits

- No orchestrator yet: runs are started by hand. In production this would be an Airflow (or Fabric pipeline)
  schedule calling the same commands.
- No CI yet: the next step is a GitHub Action running `dbt build` on a small sample.
- Physical versus commercial flows are compared with JAO exchanges; ENTSO-E A09 would refine the commercial side.
