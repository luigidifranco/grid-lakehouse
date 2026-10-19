"""Silver de JAO (Publication Tool de Core): de las respuestas JSON (gzip) de bronze a tablas Delta.

Lo llama transform/silver.py. Reglas (comprobadas con datos reales, ver docs/notas-fuentes.md):
  - dateTimeUtc es el inicio del intervalo en UTC. El dominio flow-based, MaxBex y las posiciones netas
    máximas son horarios (PT60M); shadow prices, intercambios, posiciones netas, alpha y renta de
    congestión son cuartohorarios (PT15M).
  - Las publicaciones "anchas" (una columna por zona o frontera) se pasan a formato largo: una fila por
    instante y zona/frontera. Solo se guardan fronteras entre dos zonas simples (border_BE_NL); las de
    hubs virtuales HVDC (border_DK1_COBRA_NL_VH…) quedan fuera.
  - La clave natural de un CNEC es (instante, TSO, elemento, sentido, contingencia). Ninguna columna de la
    clave puede ser nula: en un MERGE, nulo = nulo no se cumple y la fila se insertaría otra vez. Por eso el
    caso base (sin contingencia) va con contingency_name = '' y un sentido que falte, con direction = ''
    (pasa en algunas reducciones de validación, p. ej. 51 filas del 07/10/2025).
  - El dominio final se carga por meses para no tener millones de filas en memoria a la vez.
"""
import gzip
import json
import re
from collections import defaultdict

import pyarrow as pa

from silver import TS, dia_mercado, ficheros, guardar, parse_ts, rel

# PTDFs que se guardan: zonas de Core y los dos lados del ALEGrO (enlace HVDC BE–DE)
ZONAS_PTDF = ["BE", "NL", "FR", "DE", "AT", "CZ", "HR", "HU", "PL", "RO", "SI", "SK", "ALBE", "ALDE"]
FRONTERA = re.compile(r"^border_([A-Z]{2})_([A-Z]{2})$")
F = pa.float64()
S = pa.string()


def registros_jao(dataset, desde, hasta):
    """(fichero, instante UTC, fila) de todos los ficheros del dataset en el rango."""
    for f in ficheros("jao", dataset, desde, hasta):
        for r in json.loads(gzip.decompress(f.read_bytes()))["data"]:
            yield f, parse_ts(r["dateTimeUtc"]), r


def por_meses(dataset, desde, hasta):
    """Agrupa los ficheros por mes, para cargar los datasets grandes en lotes."""
    meses = defaultdict(list)
    for f in ficheros("jao", dataset, desde, hasta):
        meses[f.parent.name[4:11]].append(f)
    return [meses[m] for m in sorted(meses)]


def comunes(f, ts, resolucion, ahora):
    return {"interval_start_utc": ts, "market_date": dia_mercado(ts), "resolution": resolucion,
            "source_file": rel(f), "loaded_at": ahora}


ESQ_COMUN = [("interval_start_utc", TS), ("market_date", pa.date32()), ("resolution", S)]
ESQ_FIN = [("source_file", S), ("loaded_at", TS)]


# ---------------------------------------------------------------- CNECs

def silver_dominio_final(desde, hasta, ahora):
    """Dominio flow-based final (solo CNECs no redundantes, presolved), horario."""
    numericos = {"ram": "ram_mw", "minRamFactor": "min_ram_factor_pct", "minRamTarget": "min_ram_target",
                 "imax": "imax_a", "u": "voltage_kv", "fmax": "fmax_mw", "frm": "frm_mw", "frefInit": "fref_init_mw",
                 "fnrao": "fnrao_mw", "fref": "fref_mw", "fcore": "f0_core_mw", "fall": "f0_all_mw",
                 "fuaf": "fuaf_mw", "amr": "amr_mw", "ltaMargin": "lta_margin_mw", "cva": "cva_mw", "iva": "iva_mw",
                 "ftotalLtn": "ftotal_ltn_mw", "fltn": "fltn_mw"}
    textos = {"tso": "tso", "cneName": "cne_name", "cneEic": "cne_eic", "cneStatus": "cne_status",
              "direction": "direction", "hubFrom": "hub_from", "hubTo": "hub_to", "substationFrom": "substation_from",
              "substationTo": "substation_to", "elementType": "element_type", "fmaxType": "fmax_type",
              "contTso": "contingency_tso", "justification": "justification"}
    esquema = pa.schema(ESQ_COMUN + [(c, S) for c in textos.values()] + [("contingency_name", S),
                        ("contingency_elements", pa.int32()), ("presolved", pa.bool_())]
                        + [(c, F) for c in numericos.values()] + [(f"ptdf_{z}", F) for z in ZONAS_PTDF] + ESQ_FIN)
    clave = ["interval_start_utc", "tso", "cne_name", "direction", "contingency_name"]
    total = 0
    for lote in por_meses("dominio_final", desde, hasta):
        filas = {}
        for f in lote:
            for r in json.loads(gzip.decompress(f.read_bytes()))["data"]:
                ts = parse_ts(r["dateTimeUtc"])
                fila = comunes(f, ts, "PT60M", ahora)
                fila.update({d: (r.get(o) or "").strip() if d in ("cne_name",) else r.get(o) for o, d in textos.items()})
                fila["tso"] = (r.get("tso") or "").upper()
                fila["direction"] = r.get("direction") or ""
                fila["contingency_name"] = (r.get("contName") or "").strip()
                fila["contingency_elements"] = len([c for c in (r.get("contingencies") or []) if c.get("branchName")])
                fila["presolved"] = r.get("presolved")
                fila.update({d: r.get(o) for o, d in numericos.items()})
                fila.update({f"ptdf_{z}": r.get(f"ptdf_{z}") for z in ZONAS_PTDF})
                filas[tuple(fila[c] for c in clave)] = fila
        guardar("jao_final_domain", list(filas.values()), esquema, clave)
        total += len(filas)
    return total


def silver_shadow_prices(desde, hasta, ahora):
    """CNECs activos tras el acoplamiento de mercado, por cuarto de hora, con su shadow price."""
    numericos = {"shadowPrice": "shadow_price_eur_mw", "ram": "ram_mw", "ramMcp": "ram_mcp_mw", "imax": "imax_a",
                 "fmax": "fmax_mw", "frm": "frm_mw", "fref": "fref_mw", "f0core": "f0_core_mw", "f0all": "f0_all_mw",
                 "fuaf": "fuaf_mw", "amr": "amr_mw", "ltaMargin": "lta_margin_mw", "cva": "cva_mw", "iva": "iva_mw",
                 "ftotalLtn": "ftotal_ltn_mw", "minRamFactor": "min_ram_factor_pct", "maxZ2ZPtdf": "max_z2z_ptdf"}
    esquema = pa.schema(ESQ_COMUN + [(c, S) for c in ("tso", "cnec_name", "cnec_eic", "direction",
                        "contingency_name", "branch_eic", "hub_from", "hub_to")]
                        + [(c, F) for c in numericos.values()] + [(f"ptdf_{z}", F) for z in ZONAS_PTDF] + ESQ_FIN)
    clave = ["interval_start_utc", "tso", "cnec_name", "direction", "contingency_name"]
    filas = {}
    for f, ts, r in registros_jao("shadow_prices", desde, hasta):
        fila = comunes(f, ts, "PT15M", ahora)
        fila.update({"tso": (r.get("tso") or "").upper(), "cnec_name": (r.get("cnecName") or "").strip(),
                     "cnec_eic": r.get("cnecEic"), "direction": r.get("direction") or "",
                     "contingency_name": (r.get("contName") or "").strip(), "branch_eic": r.get("branchEic"),
                     "hub_from": r.get("hubFrom"), "hub_to": r.get("hubTo")})
        fila.update({d: r.get(o) for o, d in numericos.items()})
        fila.update({f"ptdf_{z}": r.get(f"hub_{z}") for z in ZONAS_PTDF})
        filas[tuple(fila[c] for c in clave)] = fila
    guardar("jao_shadow_price", list(filas.values()), esquema, clave)
    return len(filas)


def silver_reducciones(desde, hasta, ahora):
    """Reducciones de capacidad por validación (IVA) de cada TSO, con su justificación."""
    esquema = pa.schema(ESQ_COMUN + [("tso", S), ("cnec_name", S), ("direction", S), ("iva_mw", F),
                        ("iva_share", F), ("justification", S), ("comment", S), ("is_returned_branch", pa.bool_()),
                        ("fallback_applied", pa.bool_())] + ESQ_FIN)
    clave = ["interval_start_utc", "tso", "cnec_name", "direction"]
    filas = {}
    for f, ts, r in registros_jao("reducciones_validacion", desde, hasta):
        fila = comunes(f, ts, "PT60M", ahora)
        fila.update({"tso": (r.get("tso") or "").upper(), "cnec_name": (r.get("cnecName") or "").strip(),
                     "direction": r.get("direction") or "", "iva_mw": r.get("iva"), "iva_share": r.get("ivaShare"),
                     "justification": r.get("justification"), "comment": r.get("comment"),
                     "is_returned_branch": r.get("isReturnedBranch"), "fallback_applied": r.get("fallbackApplied")})
        filas[tuple(fila[c] for c in clave)] = fila
    guardar("jao_validation_reduction", list(filas.values()), esquema, clave)
    return len(filas)


# ---------------------------------------------------------------- publicaciones anchas → largas

def silver_por_frontera(dataset, tabla, resolucion, desde, hasta, ahora):
    esquema = pa.schema(ESQ_COMUN + [("from_zone", S), ("to_zone", S), ("mw", F)] + ESQ_FIN)
    filas = {}
    for f, ts, r in registros_jao(dataset, desde, hasta):
        for col, valor in r.items():
            m = FRONTERA.match(col)
            if m:
                fila = comunes(f, ts, resolucion, ahora)
                fila.update({"from_zone": m.group(1), "to_zone": m.group(2), "mw": valor})
                filas[(ts, m.group(1), m.group(2))] = fila
    guardar(tabla, list(filas.values()), esquema, ["interval_start_utc", "from_zone", "to_zone"])
    return len(filas)


def silver_posicion_neta(desde, hasta, ahora):
    esquema = pa.schema(ESQ_COMUN + [("zone", S), ("net_position_mw", F)] + ESQ_FIN)
    filas = {}
    for f, ts, r in registros_jao("posicion_neta", desde, hasta):
        for col, valor in r.items():
            if col.startswith("hub_"):
                fila = comunes(f, ts, "PT15M", ahora)
                fila.update({"zone": col[4:], "net_position_mw": valor})
                filas[(ts, col[4:])] = fila
    guardar("jao_net_position", list(filas.values()), esquema, ["interval_start_utc", "zone"])
    return len(filas)


def silver_posicion_neta_max(desde, hasta, ahora):
    esquema = pa.schema(ESQ_COMUN + [("zone", S), ("min_net_position_mw", F), ("max_net_position_mw", F)] + ESQ_FIN)
    filas = {}
    for f, ts, r in registros_jao("posicion_neta_max", desde, hasta):
        for col, valor in r.items():
            if col[:3] in ("min", "max") and col != "id":
                zona = col[3:]
                fila = filas.setdefault((ts, zona), {**comunes(f, ts, "PT60M", ahora), "zone": zona})
                fila[f"{col[:3]}_net_position_mw"] = valor
    guardar("jao_max_net_position", list(filas.values()), esquema, ["interval_start_utc", "zone"])
    return len(filas)


def silver_alpha(desde, hasta, ahora):
    esquema = pa.schema(ESQ_COMUN + [("alpha_factor", F)] + ESQ_FIN)
    filas = {}
    for f, ts, r in registros_jao("alpha", desde, hasta):
        filas[ts] = {**comunes(f, ts, "PT15M", ahora), "alpha_factor": r.get("alphaFactor")}
    guardar("jao_alpha_factor", list(filas.values()), esquema, ["interval_start_utc"])
    return len(filas)


def silver_renta_congestion(desde, hasta, ahora):
    """grossHub_BE, netTso_Elia, grossBorder_ES_FR… → (medida, nivel, nombre, €)."""
    patron = re.compile(r"^(gross|net)(Hub|Tso|Border)_(.+)$")
    esquema = pa.schema(ESQ_COMUN + [("measure", S), ("level", S), ("name", S), ("congestion_income_eur", F)] + ESQ_FIN)
    filas = {}
    for f, ts, r in registros_jao("renta_congestion", desde, hasta):
        for col, valor in r.items():
            m = patron.match(col)
            if m:
                medida, nivel, nombre = m.group(1), m.group(2).lower(), m.group(3)
                nombre = nombre.upper() if nivel == "tso" else nombre
                filas[(ts, medida, nivel, nombre)] = {**comunes(f, ts, "PT15M", ahora), "measure": medida,
                                                      "level": nivel, "name": nombre, "congestion_income_eur": valor}
    guardar("jao_congestion_income", list(filas.values()), esquema, ["interval_start_utc", "measure", "level", "name"])
    return len(filas)


def construir(desde, hasta, ahora):
    """Construye todas las tablas silver de JAO. Devuelve {tabla: filas}."""
    return {
        "jao_final_domain": silver_dominio_final(desde, hasta, ahora),
        "jao_shadow_price": silver_shadow_prices(desde, hasta, ahora),
        "jao_validation_reduction": silver_reducciones(desde, hasta, ahora),
        "jao_scheduled_exchange": silver_por_frontera("intercambios_programados", "jao_scheduled_exchange",
                                                      "PT15M", desde, hasta, ahora),
        "jao_maxbex": silver_por_frontera("maxbex", "jao_maxbex", "PT60M", desde, hasta, ahora),
        "jao_net_position": silver_posicion_neta(desde, hasta, ahora),
        "jao_max_net_position": silver_posicion_neta_max(desde, hasta, ahora),
        "jao_alpha_factor": silver_alpha(desde, hasta, ahora),
        "jao_congestion_income": silver_renta_congestion(desde, hasta, ahora),
    }
