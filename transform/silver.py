#!/usr/bin/env python3
"""Capa silver: convierte las respuestas crudas de bronze en tablas Delta limpias, una fila por cuarto de hora.

Reglas aplicadas (comprobadas con datos reales, ver docs/notas-fuentes.md):
  - Todo en UTC; cada fila lleva además su día de mercado (hora de Bruselas).
  - ENTSO-E (curveType A03) omite posiciones: se rellenan con el último valor del mismo Period, nunca con 0.
  - ENTSO-E A44 repite TimeSeries idénticas: se deduplica por clave; si dos copias discrepan, se para con error.
  - DE_LU trae dos subastas (classificationSequence 1 y 2): se guardan las dos, con la secuencia en la clave.
  - Elia solar mezcla país, regiones y provincias: se guarda el nivel de cada fila para no sumar de más.
  - Carga idempotente: MERGE por clave natural, así repetir un día no duplica nada.

Uso (desde la carpeta del proyecto, con el entorno virtual activado):
  python transform/silver.py                      todos los días que haya en bronze
  python transform/silver.py --desde 2026-10-11 --hasta 2026-10-12
"""
import argparse
import datetime as dt
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

RAIZ = Path(__file__).resolve().parent.parent
BRONZE = RAIZ / "data" / "bronze"
SILVER = RAIZ / "data" / "silver"
BRUSELAS = ZoneInfo("Europe/Brussels")
UTC = dt.timezone.utc
CUARTO = dt.timedelta(minutes=15)

# Códigos EIC → código corto, para que las tablas sean legibles
EIC = {
    "10YBE----------2": "BE", "10YFR-RTE------C": "FR", "10YNL----------L": "NL",
    "10Y1001A1001A82H": "DE_LU", "10Y1001A1001A83F": "DE", "10YLU-CEGEDEL-NQ": "LU", "10YGB----------A": "GB",
}
PAIS_ELIA = {"France": "FR", "Netherlands": "NL", "Germany": "DE", "Luxembourg": "LU", "UnitedKingdom": "GB"}
REGIONES = {"Flanders", "Wallonia", "Brussels"}
TS = pa.timestamp("us", tz="UTC")


class ErrorCalidad(Exception):
    pass


# ---------------------------------------------------------------- utilidades

def dia_mercado(ts_utc):
    return ts_utc.astimezone(BRUSELAS).date()


def cuartos_del_dia(dia):
    """92, 96 o 100 según haya cambio de hora ese día."""
    inicio = dt.datetime.combine(dia, dt.time(0), tzinfo=BRUSELAS).astimezone(UTC)
    fin = dt.datetime.combine(dia + dt.timedelta(days=1), dt.time(0), tzinfo=BRUSELAS).astimezone(UTC)
    return int((fin - inicio) / CUARTO)


def ficheros(fuente, dataset, desde, hasta):
    """Ficheros de bronze de un dataset dentro del rango de días."""
    for f in sorted((BRONZE / fuente / dataset).glob("*/dia=*/respuesta.*")):
        dia = dt.date.fromisoformat(f.parent.name.split("=", 1)[1])
        if (desde is None or dia >= desde) and (hasta is None or dia <= hasta):
            yield f


def rel(f):
    return str(f.relative_to(RAIZ))


def parse_ts(texto):
    """'2026-10-11T22:00Z' (ENTSO-E) o '2026-10-11T22:00:00+00:00' (Elia) → datetime UTC."""
    return dt.datetime.fromisoformat(texto.replace("Z", "+00:00")).astimezone(UTC)


def resolucion(texto):
    return {"PT15M": dt.timedelta(minutes=15), "PT30M": dt.timedelta(minutes=30),
            "PT60M": dt.timedelta(hours=1)}[texto]


def guardar(nombre, filas, esquema, clave):
    """MERGE idempotente en la tabla Delta silver/<nombre> por la clave natural."""
    if not filas:
        print(f"  {nombre}: sin filas")
        return
    tabla = pa.Table.from_pylist(filas, schema=esquema)
    ruta = SILVER / nombre
    if not (ruta / "_delta_log").exists():
        write_deltalake(str(ruta), tabla, partition_by=["market_date"])
        print(f"  {nombre}: creada con {tabla.num_rows:,} filas")
        return
    condicion = " AND ".join(f"t.{c} = s.{c}" for c in clave)
    res = (DeltaTable(str(ruta)).merge(tabla, condicion, source_alias="s", target_alias="t")
           .when_matched_update_all().when_not_matched_insert_all().execute())
    print(f"  {nombre}: {res.get('num_target_rows_inserted', 0):,} insertadas, "
          f"{res.get('num_target_rows_updated', 0):,} actualizadas")


# ---------------------------------------------------------------- ENTSO-E

def series_entsoe(fichero, campo_valor):
    """Devuelve [(atributos de la serie, [(inicio_utc, valor, resolución)])], con huecos A03 rellenados."""
    raiz = ET.parse(fichero).getroot()
    local = lambda el: el.tag.split("}")[-1]
    if local(raiz) == "Acknowledgement_MarketDocument":
        return []
    salida = []
    for serie in (el for el in raiz if local(el) == "TimeSeries"):
        attrs = {local(h): (h.text or "").strip() for h in serie if len(h) == 0}
        puntos = []
        for periodo in (el for el in serie if local(el) == "Period"):
            hijo = lambda nombre: next(el.text for el in periodo.iter() if local(el) == nombre)
            inicio, fin, res = parse_ts(hijo("start")), parse_ts(hijo("end")), hijo("resolution")
            paso = resolucion(res)
            valores = {int(p.find("./{*}position").text): float(p.find(f"./{{*}}{campo_valor}").text)
                       for p in periodo.iter() if local(p) == "Point"}
            n = int((fin - inicio) / paso)
            ultimo = None
            for pos in range(1, n + 1):
                if pos in valores:
                    ultimo = valores[pos]
                if ultimo is None:  # la serie empieza con hueco: no inventamos nada
                    continue
                puntos.append((inicio + (pos - 1) * paso, ultimo, res))
        salida.append((attrs, puntos))
    return salida


def silver_precios(desde, hasta, ahora):
    filas = {}
    for f in ficheros("entsoe", "a44_precios", desde, hasta):
        for attrs, puntos in series_entsoe(f, "price.amount"):
            zona = EIC.get(attrs.get("in_Domain.mRID"), attrs.get("in_Domain.mRID"))
            # DE_LU publica dos subastas distintas el mismo día, marcadas con classificationSequence (1 = la
            # principal, SDAC). Son precios diferentes, no duplicados: la secuencia forma parte de la clave.
            secuencia = int(attrs.get("classificationSequence_AttributeInstanceComponent.position") or 1)
            for ts, precio, res in puntos:
                clave = (zona, secuencia, ts)
                if clave in filas and abs(filas[clave]["price_eur_mwh"] - precio) > 1e-9:
                    raise ErrorCalidad(f"A44 {zona} {ts}: dos series con precios distintos "
                                       f"({filas[clave]['price_eur_mwh']} y {precio}) en {rel(f)}")
                filas[clave] = {"bidding_zone": zona, "auction_sequence": secuencia, "interval_start_utc": ts, "market_date": dia_mercado(ts),
                                "price_eur_mwh": precio, "resolution": res, "source_file": rel(f), "loaded_at": ahora}
    esquema = pa.schema([("bidding_zone", pa.string()), ("auction_sequence", pa.int32()), ("interval_start_utc", TS),
                         ("market_date", pa.date32()), ("price_eur_mwh", pa.float64()), ("resolution", pa.string()),
                         ("source_file", pa.string()), ("loaded_at", TS)])
    guardar("entsoe_day_ahead_price", list(filas.values()), esquema, ["bidding_zone", "auction_sequence", "interval_start_utc"])
    return list(filas.values())


def silver_flujos(desde, hasta, ahora):
    filas = {}
    for f in ficheros("entsoe", "a11_flujos", desde, hasta):
        for attrs, puntos in series_entsoe(f, "quantity"):
            origen = EIC.get(attrs.get("out_Domain.mRID"), attrs.get("out_Domain.mRID"))
            destino = EIC.get(attrs.get("in_Domain.mRID"), attrs.get("in_Domain.mRID"))
            for ts, mw, res in puntos:
                filas[(origen, destino, ts)] = {"out_area": origen, "in_area": destino, "interval_start_utc": ts,
                                                "market_date": dia_mercado(ts), "flow_mw": mw, "resolution": res,
                                                "source_file": rel(f), "loaded_at": ahora}
    esquema = pa.schema([("out_area", pa.string()), ("in_area", pa.string()), ("interval_start_utc", TS),
                         ("market_date", pa.date32()), ("flow_mw", pa.float64()), ("resolution", pa.string()),
                         ("source_file", pa.string()), ("loaded_at", TS)])
    guardar("entsoe_physical_flow", list(filas.values()), esquema, ["out_area", "in_area", "interval_start_utc"])
    return list(filas.values())


# ---------------------------------------------------------------- Elia

def registros_elia(dataset, desde, hasta):
    for f in ficheros("elia", dataset, desde, hasta):
        for r in json.loads(f.read_text(encoding="utf-8")):
            ts = parse_ts(r["datetime"])
            yield f, ts, r


def silver_consumo(desde, hasta, ahora):
    filas = {}
    for f, ts, r in registros_elia("ods001_consumo", desde, hasta):
        filas[ts] = {"interval_start_utc": ts, "market_date": dia_mercado(ts), "resolution": r["resolutioncode"],
                     "load_mw": r.get("totalload"), "forecast_most_recent_mw": r.get("mostrecentforecast"),
                     "forecast_day_ahead_mw": r.get("dayaheadforecast"),
                     "forecast_day_ahead_p10_mw": r.get("dayaheadconfidence10"),
                     "forecast_day_ahead_p90_mw": r.get("dayaheadconfidence90"),
                     "forecast_week_ahead_mw": r.get("weekaheadforecast"), "source_file": rel(f), "loaded_at": ahora}
    esquema = pa.schema([("interval_start_utc", TS), ("market_date", pa.date32()), ("resolution", pa.string())]
                        + [(c, pa.float64()) for c in ("load_mw", "forecast_most_recent_mw", "forecast_day_ahead_mw",
                                                       "forecast_day_ahead_p10_mw", "forecast_day_ahead_p90_mw",
                                                       "forecast_week_ahead_mw")]
                        + [("source_file", pa.string()), ("loaded_at", TS)])
    guardar("elia_total_load", list(filas.values()), esquema, ["interval_start_utc"])
    return list(filas.values())


def fila_renovable(f, ts, r, ahora):
    return {"interval_start_utc": ts, "market_date": dia_mercado(ts), "resolution": r["resolutioncode"],
            "measured_mw": r.get("measured"), "forecast_most_recent_mw": r.get("mostrecentforecast"),
            "forecast_day_ahead_mw": r.get("dayaheadforecast"),
            "forecast_day_ahead_p10_mw": r.get("dayaheadconfidence10"),
            "forecast_day_ahead_p90_mw": r.get("dayaheadconfidence90"),
            "monitored_capacity_mw": r.get("monitoredcapacity"), "load_factor": r.get("loadfactor"),
            "source_file": rel(f), "loaded_at": ahora}


COLUMNAS_RENOVABLE = [(c, pa.float64()) for c in ("measured_mw", "forecast_most_recent_mw", "forecast_day_ahead_mw",
                                                  "forecast_day_ahead_p10_mw", "forecast_day_ahead_p90_mw",
                                                  "monitored_capacity_mw", "load_factor")]


def silver_eolica(desde, hasta, ahora):
    filas = {}
    for f, ts, r in registros_elia("ods031_eolica", desde, hasta):
        dims = (r["offshoreonshore"], r["region"], r["gridconnectiontype"])
        filas[(ts, *dims)] = {"offshore_onshore": dims[0], "region": dims[1], "grid_connection": dims[2],
                              **fila_renovable(f, ts, r, ahora)}
    esquema = pa.schema([("offshore_onshore", pa.string()), ("region", pa.string()), ("grid_connection", pa.string()),
                         ("interval_start_utc", TS), ("market_date", pa.date32()), ("resolution", pa.string())]
                        + COLUMNAS_RENOVABLE + [("source_file", pa.string()), ("loaded_at", TS)])
    guardar("elia_wind", list(filas.values()), esquema,
            ["interval_start_utc", "offshore_onshore", "region", "grid_connection"])
    return list(filas.values())


def silver_solar(desde, hasta, ahora):
    filas = {}
    for f, ts, r in registros_elia("ods032_solar", desde, hasta):
        region = r["region"]
        nivel = "country" if region == "Belgium" else "region" if region in REGIONES else "province"
        filas[(ts, region)] = {"region": region, "region_level": nivel, **fila_renovable(f, ts, r, ahora)}
    esquema = pa.schema([("region", pa.string()), ("region_level", pa.string()), ("interval_start_utc", TS),
                         ("market_date", pa.date32()), ("resolution", pa.string())]
                        + COLUMNAS_RENOVABLE + [("source_file", pa.string()), ("loaded_at", TS)])
    guardar("elia_solar", list(filas.values()), esquema, ["interval_start_utc", "region"])
    return list(filas.values())


def silver_flujo_frontera(desde, hasta, ahora):
    filas = {}
    for f, ts, r in registros_elia("ods026_flujos_frontera", desde, hasta):
        vecino = PAIS_ELIA.get(r["controlarea"], r["controlarea"])
        filas[(ts, vecino)] = {"neighbour": vecino, "interval_start_utc": ts, "market_date": dia_mercado(ts),
                               "flow_mw": r.get("physicalflowatborder"), "resolution": r["resolutioncode"],
                               "source_file": rel(f), "loaded_at": ahora}
    esquema = pa.schema([("neighbour", pa.string()), ("interval_start_utc", TS), ("market_date", pa.date32()),
                         ("flow_mw", pa.float64()), ("resolution", pa.string()),
                         ("source_file", pa.string()), ("loaded_at", TS)])
    guardar("elia_border_flow", list(filas.values()), esquema, ["interval_start_utc", "neighbour"])
    return list(filas.values())


# ---------------------------------------------------------------- controles de calidad

def controles(precios, flujos_entsoe, consumo, solar, flujos_elia):
    avisos = []

    def completo(nombre, filas, grupo):
        cuenta = defaultdict(int)
        for r in filas:
            cuenta[(grupo(r), r["market_date"])] += 1
        for (g, dia), n in sorted(cuenta.items()):
            esperado = cuartos_del_dia(dia)
            if n != esperado:
                avisos.append(f"{nombre} {g} {dia}: {n} cuartos de hora, se esperaban {esperado}")

    completo("precios", precios, lambda r: f'{r["bidding_zone"]}#{r["auction_sequence"]}')
    completo("consumo", consumo, lambda r: "BE")
    completo("flujos Elia", flujos_elia, lambda r: r["neighbour"])

    # Solar: el total de Bélgica debe coincidir con la suma de regiones y con la de provincias (tolerancia 1 %)
    sumas = defaultdict(lambda: defaultdict(float))
    for r in solar:
        if r["measured_mw"] is not None:
            sumas[r["interval_start_utc"]][r["region_level"]] += r["measured_mw"]
    bruselas = {r["interval_start_utc"]: r["measured_mw"] or 0.0 for r in solar if r["region"] == "Brussels"}
    malos = []
    for ts, s in sorted(sumas.items()):
        # Bruselas-Capital es región pero no provincia: las 10 provincias + Bruselas = Bélgica
        for nivel, suma in (("regiones", s["region"]), ("provincias + Bruselas", s["province"] + bruselas.get(ts, 0.0))):
            if abs(suma - s["country"]) > max(1.0, 0.01 * s["country"]):
                malos.append(f"{ts:%Y-%m-%d %H:%M}Z suma de {nivel} {suma:.1f} ≠ Bélgica {s['country']:.1f}")
    if malos:
        avisos.append(f"solar: {len(malos)} cuartos de hora no cuadran, p. ej. {malos[0]}")

    # Flujos: neto ENTSO-E (BE→vecino − vecino→BE) frente a Elia, mismo convenio de signo
    entsoe = defaultdict(float)
    for r in flujos_entsoe:
        if r["out_area"] == "BE":
            entsoe[(r["in_area"], r["interval_start_utc"])] += r["flow_mw"]
        elif r["in_area"] == "BE":
            entsoe[(r["out_area"], r["interval_start_utc"])] -= r["flow_mw"]
    difs = [abs(entsoe[(r["neighbour"], r["interval_start_utc"])] - r["flow_mw"]) for r in flujos_elia
            if r["flow_mw"] is not None and (r["neighbour"], r["interval_start_utc"]) in entsoe]
    if difs:
        malos = sum(1 for d in difs if d > 1.0)
        print(f"  conciliación flujos ENTSO-E vs Elia: {len(difs):,} cuartos comparados, "
              f"{malos} con diferencia > 1 MW, máxima {max(difs):.3f} MW")
        if malos:
            avisos.append(f"flujos: {malos} cuartos de hora no cuadran entre ENTSO-E y Elia")
    return avisos


# ---------------------------------------------------------------- principal

def main():
    p = argparse.ArgumentParser(description="Bronze → silver (tablas Delta)")
    p.add_argument("--desde", type=dt.date.fromisoformat)
    p.add_argument("--hasta", type=dt.date.fromisoformat)
    a = p.parse_args()
    ahora = dt.datetime.now(UTC)
    print("Construyendo silver…")
    try:
        precios = silver_precios(a.desde, a.hasta, ahora)
    except ErrorCalidad as e:
        sys.exit(f"ERROR DE CALIDAD: {e}")
    flujos_entsoe = silver_flujos(a.desde, a.hasta, ahora)
    consumo = silver_consumo(a.desde, a.hasta, ahora)
    silver_eolica(a.desde, a.hasta, ahora)
    solar = silver_solar(a.desde, a.hasta, ahora)
    flujos_elia = silver_flujo_frontera(a.desde, a.hasta, ahora)
    print("Controles de calidad…")
    avisos = controles(precios, flujos_entsoe, consumo, solar, flujos_elia)
    for av in avisos:
        print(f"  ⚠ {av}")
    print(f"\nTerminado: {len(avisos)} avisos. Tablas en {SILVER.relative_to(RAIZ)}/")


if __name__ == "__main__":
    main()
