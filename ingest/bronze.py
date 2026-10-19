#!/usr/bin/env python3
"""Ingesta bronze: descarga día a día de ENTSO-E y Elia y guarda la respuesta cruda, sin tocarla.

Cada descarga queda en  data/bronze/<fuente>/<dataset>/<clave>/dia=<AAAA-MM-DD>/respuesta.<xml|json>
y se anota en         data/bronze/_registro_cargas.jsonl  (una línea JSON por descarga).

El "día" es el día de mercado en hora de Bruselas (00:00–24:00 locales), que en UTC va de 22:00 a 22:00
en verano y de 23:00 a 23:00 en invierno. Así un día de mercado nunca se parte en dos ficheros.

Uso (desde la carpeta del proyecto):
  python3 ingest/bronze.py --desde 2026-10-11 --hasta 2026-10-12            todas las fuentes
  python3 ingest/bronze.py --desde 2026-10-11 --hasta 2026-10-12 --solo elia
  python3 ingest/bronze.py --desde 2026-10-11 --hasta 2026-10-12 --forzar   vuelve a descargar lo ya bajado
  python3 ingest/bronze.py --desde 2025-10-01 --hasta 2026-10-12 --bloque mes   histórico: ENTSO-E por meses
  python3 ingest/bronze.py --diario                                         carga diaria: los últimos 7 días

Ventana de revisión (--revisar, 7 días por defecto): los días de mercado más recientes se vuelven a descargar
aunque ya estén, porque ENTSO-E y Elia corrigen sus datos los primeros días. Comprobado el 19/10/2026: los
días 11 y 12/10 descargados al día siguiente no cuadraban entre fuentes; descargados de nuevo, sí.

Con --bloque mes, ENTSO-E se pide un mes por petición (su API admite hasta un año) y en paralelo:
pasa de 14 peticiones por día a 14 por mes. Elia sigue por días (responde en ~1 s).

Solo usa la biblioteca estándar de Python (+ truststore si está instalado).
"""
import argparse
import calendar
import threading
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from zoneinfo import ZoneInfo

try:  # certificados del llavero de macOS (Python de python.org, antivirus o VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

RAIZ = Path(__file__).resolve().parent.parent
BRONZE = RAIZ / "data" / "bronze"
REGISTRO = BRONZE / "_registro_cargas.jsonl"
BRUSELAS = ZoneInfo("Europe/Brussels")
AGENTE = {"User-Agent": "grid-lakehouse/0.1 (portfolio project)"}  # la pasarela de ENTSO-E rechaza el de Python

ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"
ELIA_URL = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets"

# Códigos EIC. Zonas de oferta para precios; países (áreas de control) para flujos físicos.
ZONAS_PRECIO = {
    "BE": "10YBE----------2",
    "FR": "10YFR-RTE------C",
    "NL": "10YNL----------L",
    "DE_LU": "10Y1001A1001A82H",
}
BELGICA = "10YBE----------2"
VECINOS_FLUJO = {
    "FR": "10YFR-RTE------C",
    "NL": "10YNL----------L",
    "DE": "10Y1001A1001A83F",
    "LU": "10YLU-CEGEDEL-NQ",
    "GB": "10YGB----------A",
}
DATASETS_ELIA = {
    "ods001": "consumo",
    "ods031": "eolica",
    "ods032": "solar",
    "ods026": "flujos_frontera",
}


# ---------------------------------------------------------------- utilidades

def leer_token():
    if os.environ.get("ENTSOE_TOKEN"):
        return os.environ["ENTSOE_TOKEN"].strip()
    env = RAIZ / ".env"
    if env.exists():
        for linea in env.read_text().splitlines():
            if linea.startswith("ENTSOE_TOKEN="):
                return linea.split("=", 1)[1].strip()
    sys.exit("No encuentro el token de ENTSO-E: falta ENTSOE_TOKEN en .env")


def ventana_utc(dia):
    """Día de mercado en Bruselas → (inicio, fin) en UTC."""
    inicio = dt.datetime.combine(dia, dt.time(0), tzinfo=BRUSELAS)
    fin = dt.datetime.combine(dia + dt.timedelta(days=1), dt.time(0), tzinfo=BRUSELAS)
    return inicio.astimezone(dt.timezone.utc), fin.astimezone(dt.timezone.utc)


def descargar(url, intentos=4):
    """GET con reintentos ante 429 (límite de ENTSO-E) y errores 5xx o de red. Devuelve (código, bytes)."""
    espera = 5
    for intento in range(1, intentos + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=AGENTE), timeout=120) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            cuerpo = e.read()
            if e.code in (429, 500, 502, 503, 504) and intento < intentos:
                time.sleep(espera * intento)
                continue
            return e.code, cuerpo
        except urllib.error.URLError:
            if intento < intentos:
                time.sleep(espera * intento)
                continue
            raise


def sin_token(url):
    partes = urllib.parse.urlsplit(url)
    consulta = [(k, v) for k, v in urllib.parse.parse_qsl(partes.query) if k != "securityToken"]
    return urllib.parse.urlunsplit(partes._replace(query=urllib.parse.urlencode(consulta)))


def anotar(entrada):
    REGISTRO.parent.mkdir(parents=True, exist_ok=True)
    with _cerrojo, REGISTRO.open("a", encoding="utf-8") as f:   # varias descargas a la vez escriben aquí
        f.write(json.dumps(entrada, ensure_ascii=False) + "\n")


def ya_cargado(fichero):
    """Hay fichero y la última anotación sobre él terminó bien."""
    if not fichero.exists() or not REGISTRO.exists():
        return False
    rel = str(fichero.relative_to(RAIZ))
    ultimo = None
    for linea in REGISTRO.read_text(encoding="utf-8").splitlines():
        e = json.loads(linea)
        if e.get("fichero") == rel:
            ultimo = e
    return bool(ultimo) and ultimo["estado"] in ("ok", "sin_datos")


_cerrojo = threading.Lock()


def guardar_y_anotar(fuente, dataset, clave, ventana, url, codigo, cuerpo, estado, registros, t0, detalle=""):
    etiqueta, dia, dia_fin = ventana
    fichero = ruta(fuente, dataset, clave, etiqueta)
    if estado in ("ok", "sin_datos"):
        fichero.parent.mkdir(parents=True, exist_ok=True)
        fichero.write_bytes(cuerpo)
    anotar({
        "cargado_en": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "fuente": fuente, "dataset": dataset, "clave": clave, "dia_mercado": dia.isoformat(),
        "dia_mercado_fin": dia_fin.isoformat(),
        "url": sin_token(url), "http": codigo, "estado": estado, "registros": registros,
        "bytes": len(cuerpo), "sha256": hashlib.sha256(cuerpo).hexdigest(),
        "fichero": str(fichero.relative_to(RAIZ)) if estado in ("ok", "sin_datos") else None,
        "duracion_s": round(time.time() - t0, 2), "detalle": detalle[:300],
    })
    simbolo = {"ok": "✓", "sin_datos": "·", "error": "✗"}[estado]
    print(f"  {simbolo} {fuente}/{dataset}/{clave} {etiqueta} · {estado} · {registros} registros · {len(cuerpo):,} bytes"
          + (f" · {detalle[:120]}" if estado != "ok" and detalle else ""))
    return fichero


def ruta(fuente, dataset, clave, etiqueta):
    ext = "json" if fuente == "elia" else "xml"
    return BRONZE / fuente / dataset / clave / etiqueta / f"respuesta.{ext}"


def ventanas(desde, hasta, bloque):
    """(etiqueta de carpeta, primer día, último día). Por días: dia=AAAA-MM-DD; por meses: mes=AAAA-MM."""
    if bloque == "dia":
        d = desde
        while d <= hasta:
            yield f"dia={d.isoformat()}", d, d
            d += dt.timedelta(days=1)
        return
    d = desde.replace(day=1)
    while d <= hasta:
        ultimo = d.replace(day=calendar.monthrange(d.year, d.month)[1])
        ini, fin = max(d, desde), min(ultimo, hasta)
        sufijo = "" if (ini == d and fin == ultimo) else f"_{ini.day:02d}-{fin.day:02d}"  # mes incompleto
        yield f"mes={d:%Y-%m}{sufijo}", ini, fin
        d = ultimo + dt.timedelta(days=1)


# ---------------------------------------------------------------- ENTSO-E

def analizar_entsoe(cuerpo):
    """(estado, nº de TimeSeries, detalle). ENTSO-E responde 200 con un Acknowledgement cuando no hay datos."""
    try:
        raiz = ET.fromstring(cuerpo)
    except ET.ParseError as e:
        return "error", 0, f"XML ilegible: {e}"
    local = lambda el: el.tag.split("}")[-1]
    if local(raiz) == "Acknowledgement_MarketDocument":
        motivo = " ".join(el.text for el in raiz.iter() if local(el) == "text" and el.text)
        return "sin_datos", 0, motivo
    series = sum(1 for el in raiz if local(el) == "TimeSeries")
    return ("ok" if series else "sin_datos"), series, ""


def cargar_entsoe(ventana, token, forzar, hilos=1):
    etiqueta, dia_ini, dia_fin = ventana
    inicio, fin = ventana_utc(dia_ini)[0], ventana_utc(dia_fin)[1]
    periodo = {"periodStart": inicio.strftime("%Y%m%d%H%M"), "periodEnd": fin.strftime("%Y%m%d%H%M")}
    trabajos = []
    for zona, eic in ZONAS_PRECIO.items():                       # A44: precios day-ahead
        trabajos.append(("a44_precios", f"zona={zona}",
                         {"documentType": "A44", "in_Domain": eic, "out_Domain": eic}))
    for vecino, eic in VECINOS_FLUJO.items():                    # A11: flujos físicos, en los dos sentidos
        trabajos.append(("a11_flujos", f"frontera=BE_{vecino}",
                         {"documentType": "A11", "out_Domain": BELGICA, "in_Domain": eic}))
        trabajos.append(("a11_flujos", f"frontera={vecino}_BE",
                         {"documentType": "A11", "out_Domain": eic, "in_Domain": BELGICA}))
    def una(trabajo):
        dataset, clave, params = trabajo
        if not forzar and ya_cargado(ruta("entsoe", dataset, clave, etiqueta)):
            print(f"  = entsoe/{dataset}/{clave} {etiqueta} · ya estaba")
            return
        url = f"{ENTSOE_URL}?{urllib.parse.urlencode({'securityToken': token, **params, **periodo})}"
        t0 = time.time()
        codigo, cuerpo = descargar(url)
        if codigo == 200:
            estado, n, detalle = analizar_entsoe(cuerpo)
        else:
            estado, n, detalle = "error", 0, f"HTTP {codigo}: {cuerpo[:200].decode('utf-8', 'replace')}"
        guardar_y_anotar("entsoe", dataset, clave, ventana, url, codigo, cuerpo, estado, n, t0, detalle)
        time.sleep(0.5)  # ENTSO-E permite 400 peticiones/minuto; con 4 hilos seguimos muy por debajo

    with ThreadPoolExecutor(max_workers=hilos) as pool:
        list(pool.map(una, trabajos))


# ---------------------------------------------------------------- Elia

def cargar_elia(dia, forzar):
    inicio, fin = ventana_utc(dia)
    ventana = (f"dia={dia.isoformat()}", dia, dia)
    filtro = (f"datetime >= date'{inicio.strftime('%Y-%m-%dT%H:%M:%SZ')}' "
              f"and datetime < date'{fin.strftime('%Y-%m-%dT%H:%M:%SZ')}'")
    for ident, nombre in DATASETS_ELIA.items():
        dataset = f"{ident}_{nombre}"
        if not forzar and ya_cargado(ruta("elia", dataset, "todo", ventana[0])):
            print(f"  = elia/{dataset} {dia} · ya estaba")
            continue
        # El endpoint de exportación no tiene el límite de 100 registros por página del de consulta
        url = f"{ELIA_URL}/{ident}/exports/json?{urllib.parse.urlencode({'where': filtro, 'order_by': 'datetime'})}"
        t0 = time.time()
        codigo, cuerpo = descargar(url)
        if codigo == 200:
            try:
                n = len(json.loads(cuerpo))
                estado, detalle = ("ok" if n else "sin_datos"), ""
            except json.JSONDecodeError as e:
                estado, n, detalle = "error", 0, f"JSON ilegible: {e}"
        else:
            estado, n, detalle = "error", 0, f"HTTP {codigo}: {cuerpo[:200].decode('utf-8', 'replace')}"
        guardar_y_anotar("elia", dataset, "todo", ventana, url, codigo, cuerpo, estado, n, t0, detalle)
        time.sleep(0.5)


# ---------------------------------------------------------------- principal

def main():
    p = argparse.ArgumentParser(description="Ingesta bronze de ENTSO-E y Elia por día de mercado (hora de Bruselas)")
    p.add_argument("--desde", type=dt.date.fromisoformat, help="primer día, AAAA-MM-DD")
    p.add_argument("--hasta", type=dt.date.fromisoformat, help="último día, incluido")
    p.add_argument("--diario", action="store_true",
                   help="carga diaria: desde hace --revisar días hasta ayer (días completos)")
    p.add_argument("--revisar", type=int, default=7, metavar="DIAS",
                   help="volver a descargar los días de los últimos DIAS días aunque ya estén (0 = nunca)")
    p.add_argument("--solo", choices=["entsoe", "elia"], help="cargar una sola fuente")
    p.add_argument("--forzar", action="store_true", help="volver a descargar aunque ya esté")
    p.add_argument("--bloque", choices=["dia", "mes"], default="dia",
                   help="ENTSO-E por días (carga diaria) o por meses (histórico, mucho más rápido)")
    a = p.parse_args()
    hoy = dt.datetime.now(BRUSELAS).date()
    if a.diario:
        a.desde, a.hasta = hoy - dt.timedelta(days=max(a.revisar, 1)), hoy - dt.timedelta(days=1)
    if not (a.desde and a.hasta):
        sys.exit("indica --desde y --hasta, o --diario")
    if a.hasta < a.desde:
        sys.exit("--hasta no puede ser anterior a --desde")

    limite_revision = hoy - dt.timedelta(days=a.revisar)
    revisar = lambda ultimo_dia: a.forzar or ultimo_dia > limite_revision   # aún dentro de la ventana
    if a.revisar:
        print(f"Ventana de revisión: los días posteriores al {limite_revision} se vuelven a descargar")
    token = leer_token() if a.solo != "elia" else None
    if a.solo in (None, "entsoe"):
        print(f"ENTSO-E, bloques de tipo '{a.bloque}'")
        for v in ventanas(a.desde, a.hasta, a.bloque):
            ini, fin = ventana_utc(v[1])[0], ventana_utc(v[2])[1]
            print(f"{v[0]} (UTC {ini:%d/%m/%Y %H:%M} → {fin:%d/%m/%Y %H:%M})")
            cargar_entsoe(v, token, revisar(v[2]), hilos=4 if a.bloque == "mes" else 1)
    if a.solo in (None, "elia"):
        print("Elia, por días")
        dia = a.desde
        while dia <= a.hasta:
            print(f"dia={dia}")
            cargar_elia(dia, revisar(dia))
            dia += dt.timedelta(days=1)

    errores = []
    if REGISTRO.exists():
        lineas = [json.loads(l) for l in REGISTRO.read_text(encoding="utf-8").splitlines()]
        errores = [l for l in lineas if l["estado"] == "error"
                   and a.desde.isoformat() <= l["dia_mercado"] <= a.hasta.isoformat()]
    print(f"\nTerminado. Registro: {REGISTRO.relative_to(RAIZ)}" +
          (f" · {len(errores)} errores en este rango (revisa las líneas con ✗)" if errores else " · sin errores"))


if __name__ == "__main__":
    main()
