#!/usr/bin/env python3
"""Explora el catálogo de Elia Open Data (opendata.elia.be). Solo usa la biblioteca estándar.

Uso:
  python3 scripts/explorar_elia.py                 → lista todos los datasets (id, registros, título)
  python3 scripts/explorar_elia.py load solar      → solo los que contienen esas palabras en el título
  python3 scripts/explorar_elia.py --ver ods001     → campos y 3 registros de ejemplo de un dataset
"""
import json
import sys
import urllib.parse
import urllib.request

try:  # usa los certificados del llavero de macOS (necesario con el Python de python.org o con antivirus/VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

BASE = "https://opendata.elia.be/api/explore/v2.1"


def pedir(ruta, **params):
    url = f"{BASE}{ruta}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def catalogo():
    datos, offset = [], 0
    while True:
        pagina = pedir("/catalog/datasets", limit=100, offset=offset)
        datos += pagina["results"]
        offset += 100
        if offset >= pagina["total_count"]:
            return datos


def listar(palabras):
    filas = []
    for d in catalogo():
        meta = d.get("metas", {}).get("default", {})
        titulo = meta.get("title", "")
        if all(p.lower() in titulo.lower() for p in palabras):
            filas.append((d["dataset_id"], meta.get("records_count") or 0, titulo))
    for ident, n, titulo in sorted(filas):
        print(f"{ident:10} {n:>12,}  {titulo}")
    print(f"\n{len(filas)} datasets")


def ver(ident):
    d = pedir(f"/catalog/datasets/{ident}")
    meta = d.get("metas", {}).get("default", {})
    print(meta.get("title"), "\n")
    print("Campos:")
    for f in d.get("fields", []):
        print(f"  {f['name']:35} {f['type']:10} {f.get('label', '')}")
    muestra = pedir(f"/catalog/datasets/{ident}/records", limit=3)
    print(f"\nRegistros totales: {muestra.get('total_count')}\nEjemplo:")
    print(json.dumps(muestra.get("results", []), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["--ver"] and len(args) == 2:
        ver(args[1])
    else:
        listar(args)
