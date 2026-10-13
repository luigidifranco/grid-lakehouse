#!/usr/bin/env python3
"""Prueba rápida del token de ENTSO-E: descarga los precios day-ahead de Bélgica de ayer.

El token se lee de la variable ENTSOE_TOKEN o del fichero .env (que nunca se sube a Git).
Uso:  python3 scripts/probar_entsoe.py
"""
import datetime as dt
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

try:  # usa los certificados del llavero de macOS (necesario con el Python de python.org o con antivirus/VPN que inspeccionan https)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass
import xml.etree.ElementTree as ET
from pathlib import Path

URL = "https://web-api.tp.entsoe.eu/api"
BELGICA = "10YBE----------2"   # código EIC de la zona de oferta belga


def leer_token():
    if os.environ.get("ENTSOE_TOKEN"):
        return os.environ["ENTSOE_TOKEN"].strip()
    env = Path(__file__).resolve().parent.parent / ".env"
    if env.exists():
        for linea in env.read_text().splitlines():
            if linea.startswith("ENTSOE_TOKEN="):
                return linea.split("=", 1)[1].strip()
    sys.exit("No encuentro el token: falta ENTSOE_TOKEN en .env")


def main():
    hoy = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    ayer = hoy - dt.timedelta(days=1)
    params = {
        "securityToken": leer_token(),
        "documentType": "A44",                     # precios day-ahead
        "in_Domain": BELGICA, "out_Domain": BELGICA,
        "periodStart": ayer.strftime("%Y%m%d%H%M"),
        "periodEnd": hoy.strftime("%Y%m%d%H%M"),
    }
    try:
        # El cortafuegos de ENTSO-E (Azure) rechaza con 403 el agente por defecto de Python: nos identificamos
        peticion = urllib.request.Request(f"{URL}?{urllib.parse.urlencode(params)}",
                                          headers={"User-Agent": "grid-lakehouse/0.1 (portfolio project)"})
        with urllib.request.urlopen(peticion, timeout=60) as r:
            xml = r.read()
    except urllib.error.HTTPError as e:
        cuerpo = e.read().decode("utf-8", "replace")
        sys.exit(f"Error HTTP {e.code}. Respuesta de ENTSO-E:\n{cuerpo[:800]}")

    # Guardamos la respuesta cruda (esto será la capa bronze): data/ no se sube a Git
    muestra = Path(__file__).resolve().parent.parent / "data" / "muestras" / f"a44_BE_{ayer:%Y%m%d}.xml"
    muestra.parent.mkdir(parents=True, exist_ok=True)
    muestra.write_bytes(xml)
    print(f"XML guardado en {muestra.relative_to(muestra.parents[2])}\n")

    raiz = ET.fromstring(xml)
    local = lambda el: el.tag.split("}")[-1]
    if local(raiz) == "Acknowledgement_MarketDocument":
        textos = [el.text for el in raiz.iter() if local(el) == "text"]
        sys.exit(f"ENTSO-E responde sin datos: {' '.join(t for t in textos if t)}")

    # Una respuesta trae varias TimeSeries; cada una con su Period (intervalo en UTC) y sus puntos
    for serie in (el for el in raiz.iter() if local(el) == "TimeSeries"):
        datos = {local(h): (h.text or "").strip() for h in serie if len(h) == 0}
        for periodo in (el for el in serie if local(el) == "Period"):
            inicio = next(el.text for el in periodo.iter() if local(el) == "start")
            fin = next(el.text for el in periodo.iter() if local(el) == "end")
            res = next(el.text for el in periodo.iter() if local(el) == "resolution")
            puntos = [(int(p.find("./{*}position").text), float(p.find("./{*}price.amount").text))
                      for p in periodo.iter() if local(p) == "Point"]
            precios = [v for _, v in puntos]
            extra = {k: v for k, v in datos.items() if k not in ("mRID", "businessType", "currency_Unit.name",
                     "price_Measure_Unit.name", "curveType", "in_Domain.mRID", "out_Domain.mRID")}
            print(f"Serie {datos.get('mRID')} · {inicio} → {fin} · {res} · {len(puntos)} puntos · {extra}")
            print(f"  mínimo {min(precios):.2f} €/MWh · media {sum(precios)/len(precios):.2f} · máximo {max(precios):.2f}")
    print(f"\nToken correcto. Tamaño del XML recibido: {len(xml):,} bytes")


if __name__ == "__main__":
    main()
