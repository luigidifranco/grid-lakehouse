#!/usr/bin/env python3
"""Capa de servicio: exporta las tablas de gold que usa el informe de Power BI como tablas Delta.

Salida, sobrescrita en cada ejecución:
  - data/serving/<tabla>/  tablas Delta y _manifest.json. Se suben tal cual a la sección Tables/ de un
    Lakehouse de Microsoft Fabric, donde un modelo semántico en modo Direct Lake las lee sin copiarlas.
  - data/serving_parquet/<tabla>.parquet  un fichero por tabla, para Power BI Desktop (conector Parquet,
    modo Import) desde el PC con Windows, a través de la carpeta compartida del Mac. Power BI Desktop no
    lee bien tablas Delta de una carpeta local; Parquet sí. Aquí la hora local va sin zona (datetime).

Ajustes para Power BI (comprobados al diseñar el modelo, ver docs/report-design.md):
  - Las tablas con interval_start_utc ganan interval_start_local (hora de pared de Bruselas), hour_local y
    quarter_of_day, para que los gráficos por hora del día no dependan del cambio de hora. La hora local se
    guarda como timestamp "UTC" con el valor de pared de Bruselas: así Power BI la muestra tal cual y se evita
    el tipo timestamp_ntz de Delta, que no todos los lectores aceptan.
  - Los enteros de 128 bits de DuckDB (sumas) pasan a int64 y los decimales a double.

Uso (desde la carpeta del proyecto, con el entorno virtual activado y después de dbt build):
  python serve/export_gold.py
"""
import datetime as dt
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

RAIZ = Path(__file__).resolve().parent.parent
GOLD = RAIZ / "data" / "gold" / "grid.duckdb"
SALIDA = RAIZ / "data" / "serving"
SALIDA_PARQUET = RAIZ / "data" / "serving_parquet"

# tabla publicada → origen en grid.duckdb. Dimensiones primero, luego hechos y marts.
TABLAS = {
    "dim_date": "gold.dim_date",
    "dim_border": "reference.borders",
    "dim_bidding_zone": "reference.bidding_zones",
    "dim_tso": "reference.tsos",
    "fct_day_ahead_price": "gold.fct_day_ahead_price",
    "fct_be_system": "gold.fct_be_system",
    "fct_cross_border_flow": "gold.fct_cross_border_flow",
    "fct_border_flow_price": "gold.fct_border_flow_price",
    "fct_border_commercial_physical": "gold.fct_border_commercial_physical",
    "fct_cnec_hourly": "gold.fct_cnec_hourly",
    "mart_daily_flow_reconciliation": "gold.mart_daily_flow_reconciliation",
    "mart_daily_price_spread": "gold.mart_daily_price_spread",
    "mart_daily_forecast_accuracy": "gold.mart_daily_forecast_accuracy",
    "mart_monthly_border_flow_price": "gold.mart_monthly_border_flow_price",
    "mart_monthly_unscheduled_flow": "gold.mart_monthly_unscheduled_flow",
    "mart_monthly_min_ram": "gold.mart_monthly_min_ram",
    "mart_monthly_active_constraints": "gold.mart_monthly_active_constraints",
}


def consulta(con, origen):
    columnas = [c[0] for c in con.sql(f"describe {origen}").fetchall()]
    if "interval_start_utc" not in columnas:
        return f"select * from {origen}"
    local = "timezone('Europe/Brussels', interval_start_utc)"   # hora de pared de Bruselas, sin zona
    extra = [f"hour({local})::int as hour_local", f"(hour({local}) * 4 + minute({local}) // 15)::int as quarter_of_day"]
    if "interval_start_local" not in columnas:                   # fct_be_system ya la trae
        extra.insert(0, f"{local} as interval_start_local")
    return f"select *, {', '.join(extra)} from {origen}"


def normalizar(tabla):
    """Tipos que Delta/Power BI leen sin sorpresas."""
    campos = []
    for f in tabla.schema:
        t = f.type
        if pa.types.is_decimal(t):
            t = pa.float64()
        elif pa.types.is_integer(t) and t.bit_width > 64:
            t = pa.int64()
        elif pa.types.is_timestamp(t) and t.tz is None:   # hora de pared sin zona → misma hora marcada como UTC
            t = pa.timestamp("us", tz="UTC")
        elif pa.types.is_timestamp(t):
            t = pa.timestamp("us", tz="UTC")
        campos.append(pa.field(f.name, t))
    return tabla.cast(pa.schema(campos))


def para_parquet(tabla):
    """Para Power BI Desktop: la hora local, sin zona, para que Power Query la lea como datetime."""
    if "interval_start_local" not in tabla.column_names:
        return tabla
    i = tabla.schema.get_field_index("interval_start_local")
    return tabla.set_column(i, pa.field("interval_start_local", pa.timestamp("us")),
                            tabla.column(i).cast(pa.timestamp("us")))


def main():
    SALIDA.mkdir(parents=True, exist_ok=True)
    SALIDA_PARQUET.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(GOLD), read_only=True)
    con.sql("set TimeZone = 'UTC'")
    manifiesto = {"exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "tables": {}}
    for nombre, origen in TABLAS.items():
        rel = con.sql(consulta(con, origen))
        tabla = normalizar(rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table())
        write_deltalake(str(SALIDA / nombre), tabla, mode="overwrite", schema_mode="overwrite")
        pq.write_table(para_parquet(tabla), SALIDA_PARQUET / f"{nombre}.parquet", compression="snappy")
        manifiesto["tables"][nombre] = {"source": origen, "rows": tabla.num_rows, "columns": tabla.num_columns}
        print(f"  {nombre:34} {tabla.num_rows:>10,} filas  {tabla.num_columns:>3} columnas")
    (SALIDA / "_manifest.json").write_text(json.dumps(manifiesto, indent=2), encoding="utf-8")
    print(f"\nTerminado: {len(TABLAS)} tablas Delta en {SALIDA.relative_to(RAIZ)}/ "
          f"y {len(TABLAS)} ficheros Parquet en {SALIDA_PARQUET.relative_to(RAIZ)}/")


if __name__ == "__main__":
    main()
