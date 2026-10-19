#!/usr/bin/env python3
"""Explora la API del JAO Publication Tool de Core (publicationtool.jao.eu/core/api). Solo biblioteca estándar.

Para cada publicación pide un día de mercado (o una hora, en las de dominio flow-based, que son muy grandes)
y muestra: código HTTP, filas totales, tiempo de respuesta, columnas y 2 filas de ejemplo.

Uso:  python3 scripts/explorar_jao.py > docs/catalogo_jao.txt 2>&1
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

try:  # certificados del llavero de macOS (antivirus o VPN que inspeccionan https), como en ingest/bronze.py
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

BASE = "https://publicationtool.jao.eu/core/api/data/"
# Día de mercado 11/10/2026 en hora de Bruselas = 10/10 22:00Z → 11/10 22:00Z
DIA = ("2026-10-10T22:00:00.000Z", "2026-10-11T22:00:00.000Z")
HORA = ("2026-10-11T10:00:00.000Z", "2026-10-11T11:00:00.000Z")   # 12:00–13:00 en Bruselas

# (endpoint, ventana, estilo de parámetros). Las de dominio usan FromUtc/ToUtc con Skip/Take;
# el resto, FromUTC/ToUTC (así lo hace la librería jao-py).
PUBLICACIONES = [
    ("shadowPrices", DIA, "base"),            # CNECs activos tras el acoplamiento, con su shadow price
    ("maxNetPos", DIA, "base"),               # posición neta mín./máx. por zona
    ("maxExchanges", DIA, "base"),            # MaxBex: intercambio bilateral máximo entre zonas
    ("netPos", DIA, "base"),                  # posición neta resultante del mercado
    ("scheduledExchanges", DIA, "base"),      # intercambios comerciales programados (para comparar con físicos)
    ("validationReductions", DIA, "base"),    # reducciones IVA/CVA de los TSOs
    ("priceSpread", DIA, "base"),
    ("congestionIncome", DIA, "base"),
    ("alphaFactor", DIA, "base"),
    ("finalComputation", HORA, "dominio"),    # dominio flow-based final: RAM, Fmax, FRM, F0, AMR, PTDFs…
]


def pedir(endpoint, ventana, estilo, take=None):
    if estilo == "dominio":
        params = {"FromUtc": ventana[0], "ToUtc": ventana[1], "Skip": 0, "Take": take if take is not None else 0}
    else:
        params = {"FromUTC": ventana[0], "ToUTC": ventana[1]}
    url = BASE + endpoint + "?" + urllib.parse.urlencode(params)
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=120) as r:
            cuerpo, codigo = r.read(), r.status
    except urllib.error.HTTPError as e:
        cuerpo, codigo = e.read(), e.code
    except Exception as e:  # sin red, timeout…
        return None, f"{type(e).__name__}: {e}", time.time() - t0, 0
    try:
        return json.loads(cuerpo), codigo, time.time() - t0, len(cuerpo)
    except json.JSONDecodeError:
        return None, f"{codigo} no es JSON: {cuerpo[:200]!r}", time.time() - t0, len(cuerpo)


for endpoint, ventana, estilo in PUBLICACIONES:
    print(f"\n=== {endpoint}  ({ventana[0]} → {ventana[1]})")
    datos, codigo, seg, nbytes = pedir(endpoint, ventana, estilo, take=3 if estilo == "dominio" else None)
    print(f"HTTP {codigo} · {seg:.1f} s · {nbytes:,} bytes")
    if not isinstance(datos, dict):
        print(f"respuesta: {str(datos)[:300]}")
        continue
    filas = datos.get("data") or []
    print(f"claves de la respuesta: {list(datos.keys())}")
    print(f"filas totales: {datos.get('totalRowsWithFilter', len(filas))} · devueltas: {len(filas)}")
    if filas:
        print(f"columnas ({len(filas[0])}): {list(filas[0].keys())}")
        for f in filas[:2]:
            print(json.dumps(f, ensure_ascii=False)[:1500])
    time.sleep(1)   # JAO permite 100 peticiones por minuto
