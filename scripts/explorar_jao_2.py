#!/usr/bin/env python3
"""Segunda ronda de exploración de JAO (Core):
1. Detalle del error 400 de shadowPrices y validationReductions, y reintento con otros parámetros.
2. Tamaño del dominio flow-based final (finalComputation) con filtros: no redundantes (presolved) y CNECs de Elia.

Uso:  python scripts/explorar_jao_2.py > docs/catalogo_jao_2.txt 2>&1
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

try:  # certificados del llavero de macOS (antivirus o VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

BASE = "https://publicationtool.jao.eu/core/api/data/"
DIA = ("2026-10-10T22:00:00.000Z", "2026-10-11T22:00:00.000Z")
HORA = ("2026-10-11T10:00:00.000Z", "2026-10-11T11:00:00.000Z")


def pedir(endpoint, params, muestra=2):
    url = BASE + endpoint + "?" + urllib.parse.urlencode(params)
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=180) as r:
            cuerpo, codigo = r.read(), r.status
    except urllib.error.HTTPError as e:
        cuerpo, codigo = e.read(), e.code
    seg = time.time() - t0
    print(f"\n--- {endpoint} {params}")
    print(f"HTTP {codigo} · {seg:.1f} s · {len(cuerpo):,} bytes")
    try:
        datos = json.loads(cuerpo)
    except json.JSONDecodeError:
        print(cuerpo[:400])
        return
    if codigo != 200:
        print(json.dumps(datos, ensure_ascii=False)[:800])
        return
    filas = datos.get("data") or []
    print(f"totalRowsWithFilter={datos.get('totalRowsWithFilter')} totalRows={datos.get('totalRows')} devueltas={len(filas)}")
    if filas:
        print(f"columnas ({len(filas[0])}): {list(filas[0].keys())}")
        for f in filas[:muestra]:
            f = {k: v for k, v in f.items() if not k.startswith("ptdf_")}   # sin PTDFs, para leer mejor
            print(json.dumps(f, ensure_ascii=False)[:1200])
    time.sleep(1)


# 1. Las dos que dieron 400: ver el error y probar con FromUtc/ToUtc + Skip/Take
for ep in ("shadowPrices", "validationReductions"):
    pedir(ep, {"FromUTC": DIA[0], "ToUTC": DIA[1]})
    pedir(ep, {"FromUtc": DIA[0], "ToUtc": DIA[1], "Skip": 0, "Take": 3})

# 2. Dominio final: cuántas filas hay con cada filtro (Take=0 solo cuenta)
pedir("finalComputation", {"FromUtc": HORA[0], "ToUtc": HORA[1], "Skip": 0, "Take": 0})
pedir("finalComputation", {"FromUtc": HORA[0], "ToUtc": HORA[1], "Skip": 0, "Take": 3,
                           "Filter": json.dumps({"Presolved": True})})
pedir("finalComputation", {"FromUtc": HORA[0], "ToUtc": HORA[1], "Skip": 0, "Take": 3,
                           "Filter": json.dumps({"Tso": ["ELIA"]})})
pedir("finalComputation", {"FromUtc": DIA[0], "ToUtc": DIA[1], "Skip": 0, "Take": 0,
                           "Filter": json.dumps({"Presolved": True})})
pedir("finalComputation", {"FromUtc": DIA[0], "ToUtc": DIA[1], "Skip": 0, "Take": 0,
                           "Filter": json.dumps({"Tso": ["ELIA"]})})
