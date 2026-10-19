# Notas sobre las fuentes (lo aprendido con datos reales)

## ENTSO-E · A44 precios day-ahead (probado el 13/10/2026, Bélgica `10YBE----------2`)

- **Acceso desde el Mac:** Python de python.org necesita `truststore` para usar los certificados del llavero; y la pasarela de Azure de ENTSO-E devuelve 403 al agente por defecto de Python → enviar un `User-Agent` propio.
- **Días de mercado, no días UTC:** pedir 12/10 00:00Z–13/10 00:00Z devuelve dos días de entrega completos (11/10 22:00Z–12/10 22:00Z y 12/10 22:00Z–13/10 22:00Z). La ingesta debe pedir por día de mercado en hora de Bruselas (22:00Z en verano, 23:00Z en invierno).
- **Resolución PT15M:** 96 puntos por día (92/100 los días de cambio de hora).
- **Series duplicadas:** cada día llega en dos `TimeSeries` idénticas; lo único que cambia es `mRID` (1/2, 3/4). Regla en silver: deduplicar por (zona, inicio del intervalo, posición); si dos series del mismo intervalo tuvieran precios distintos → fallo de calidad, no elegir una al azar.
- **`curveType` A03:** el estándar permite omitir posiciones cuando el precio se repite (bloques de tamaño variable). En la muestra vienen las 96, pero silver debe rellenar posiciones ausentes con el último precio y comprobar que al final hay 96 por día.
- **Cabecera:** `revisionNumber` (1) y `createdDateTime` → guardarlos en bronze para detectar revisiones publicadas más tarde.

## Elia Open Data (explorado el 13/10/2026; catálogos completos en `docs/catalogo_elia*.txt`)

Datasets elegidos (históricos; los "Near real-time" servirán para la carga diaria):

| Dataset | Contenido | Grano | Notas |
|---|---|---|---|
| ods001 | Consumo total medido + previsiones (más reciente, day-ahead 18h, week-ahead) con P10/P90 | 15 min, UTC | 413.852 registros. Permite medir el error de previsión. |
| ods031 | Eólica medida y prevista | 15 min × offshore/onshore × región × tipo de conexión | 2,07 M registros. Hay que agregar para el total nacional: comprobar antes los valores distintos de cada dimensión para no contar dos veces. `decrementalbidid` llega como la cadena literal `''` → limpiar en silver. Trae capacidad monitorizada y factor de carga. |
| ods032 | Fotovoltaica medida y prevista | igual que ods031 (por confirmar columnas) | 2,96 M registros. Evitar ods209/ods210: son datos **simulados**. |
| ods026 | Flujo físico por frontera | 15 min × país vecino (`controlarea`) | 1,63 M registros. Un solo valor por frontera: averiguar el convenio de signo (¿positivo = importación?) antes de cruzarlo con ENTSO-E A11, que da sentido por zona de entrada/salida. |
| ods134 (+ ods047) | Precios de desequilibrio por cuarto de hora | 15 min | Opcional. Histórico partido el 22/05/2024 por cambio de formato. |

Comunes a Elia:
- Fechas en UTC con `+00:00`; la API no devuelve los registros ordenados.
- Columna `resolutioncode` en todos: comprobar si el histórico antiguo es horario antes de unir.
- Nombres de país en texto (`Netherlands`, `UnitedKingdom`…) → tabla de correspondencia con los códigos EIC de ENTSO-E en silver.

## Primera carga bronze (13/10/2026, días 11 y 12/10) y comprobaciones

- 36 descargas, 0 errores. Las 5 fronteras A11 (FR, NL, DE, LU, GB) funcionan con los códigos de país.
- **Eólica (ods031):** 5 combinaciones disjuntas (offshore federal; onshore Flandes/Valonia × Elia/DSO), sin fila de total → el total nacional es la suma de las 5.
- **Solar (ods032): ¡jerarquía mezclada!** 14 "regiones" por cuarto de hora: `Belgium` (total), las 3 regiones (Flanders, Wallonia, Brussels) y las 10 provincias. Sumarlas todas multiplicaría la producción ×3. Regla: total = fila `Belgium`; test en silver: Belgium ≈ suma de regiones ≈ suma de provincias.
- **Flujos, convenio de signo:** en ods026 de Elia, **negativo = importación hacia Bélgica**, positivo = exportación. Coincide exactamente con ENTSO-E A11: neto = (BE→vecino) − (vecino→BE).
- **A11 omite posiciones (curveType A03):** un sentido puede traer solo 3 de 96 puntos. Probado contra Elia: rellenar huecos con **el último valor (forward fill)** reproduce Elia con diferencia 0 en las 5 fronteras; rellenar con 0 falla (hasta 996 MW en DE y 5 cuartos en GB). Regla de silver: forward fill dentro de cada Period, nunca 0.
- **A44:** según la zona y el día llegan 1 o 2 TimeSeries (duplicados exactos) → la deduplicación por (zona, intervalo, posición) es imprescindible.

## Capa silver (13/10/2026) — lo que encontraron los controles

- **DE_LU publica dos subastas el mismo día** (`classificationSequence` 1 y 2), con precios distintos (11/10: media 122,31 frente a 112,00 €/MWh). No son duplicados: el control "dos series con precios distintos" paró la carga y lo destapó. Regla: la secuencia forma parte de la clave (`auction_sequence`); la 1 es la subasta principal (SDAC). BE, FR y NL no la traen (secuencia 1 por defecto).
- **Solar: las 10 provincias no suman Bélgica** (≈ 2,6 % menos): falta **Bruselas-Capital**, que es región pero no provincia. Control correcto: regiones = Bélgica y provincias + Bruselas = Bélgica (tolerancia 1 %). Con eso, 0 avisos.
- **Conciliación de flujos ENTSO-E vs Elia:** 960 cuartos de hora comparados, diferencia máxima 0,000 MW.
- **Idempotencia:** volver a ejecutar silver no inserta filas nuevas (MERGE por clave natural; solo actualiza `loaded_at`).

## Reconciliación de flujos ENTSO-E (A11) vs Elia (ods026) — un año de datos (19/10/2026)

Histórico 01/10/2025–12/10/2026, 5 fronteras, sin huecos en ninguna de las dos fuentes. Hallazgos:

| Caso | Qué pasa | Tratamiento |
|---|---|---|
| NL hasta el 29/06/2026 | ENTSO-E publica la frontera NL **por horas** (PT60M); Elia, por cuartos. Desde el 30/06/2026 ENTSO-E pasa a PT15M. | Se compara el valor horario de ENTSO-E con la **media** de los 4 cuartos de Elia. Las discrepancias bajan de ~7.200 a unas 500, casi todas pequeñas. |
| Días descargados al día siguiente | 11 y 12/10/2026, descargados el 12 y 13/10, no cuadraban en ninguna frontera. Vueltos a descargar el 19/10, cuadran al 100 %. | Ventana de revisión: la ingesta vuelve a descargar los últimos 7 días (`--diario`, `--revisar`). En gold esos días salen como `provisional`. |
| NL desde el 30/06/2026 | Cuartos sueltos con picos de hasta 322 MW y media cercana a 0; van a menos (julio > agosto > septiembre). | Se marcan `relevant_deviation`. Parece un periodo de ajuste tras el cambio de resolución. |
| FR, 7 días sueltos (01–05/2026) | Todo el día desviado, ENTSO-E siempre por encima de Elia, 3–13 MW de media. | `minor_deviation` o `relevant_deviation` según el tamaño. Probablemente una revisión publicada solo en una de las fuentes. |
| GB y LU, 30/03–17/04/2026 | Diferencias pequeñas (máximo 6,5 MW), a menudo en la punta de 17:30–21:45 UTC. | `minor_deviation`. |
| GB y LU, 18/04–02/05/2026 | ENTSO-E omite a veces el sentido sin flujo. El neto coincide con Elia. | Se trata como flujo 0 en ese sentido; la cobertura se mide por frontera, no por sentido. |

Umbrales (variables en `gold/dbt_project.yml`): tolerancia 1 MW; día relevante si algún intervalo supera 50 MW
o la media diaria supera 5 MW. Resultado: DE 377 días ok; FR 370 ok / 4 menores / 3 relevantes;
GB 366 / 11 / 0; LU 365 / 12 / 0; NL 236 / 85 / 56.

Criterio: una discrepancia entre dos publicadores es información (test en `warn`); un hueco es un fallo (test que bloquea).

## Flujo físico frente a señal de precio (19/10/2026)

Modelos: `gold.fct_border_flow_price` (por cuarto de hora) y `gold.mart_monthly_border_flow_price` (por mes y frontera).
Solo fronteras acopladas en day-ahead (GB queda fuera). Flujo de Elia, precios A44 de ENTSO-E.
Umbrales en `gold/dbt_project.yml`: convergencia < 0,01 €/MWh; señal de precio ≥ 1 €/MWh; flujo ≥ 10 MW.

| Frontera | Precios convergentes | Flujo contra precio (de los cuartos con señal) | Energía contra precio | Valor contra precio |
|---|---|---|---|---|
| DE | 11,9 % | 8,8 % | 235 GWh | −2,5 M€ |
| FR | 12,4 % | 14,1 % | 1.047 GWh | −5,5 M€ |
| LU | 11,9 % | 32,7 % | 232 GWh | −2,8 M€ |
| NL | 13,1 % | **68,8 %** | 6.491 GWh | −57,7 M€ |

Lectura: Bélgica importa físicamente de NL el 94,5 % de los cuartos, también cuando es más barata que NL.
Es el tránsito norte → sur (flujos de bucle) que el cálculo flow-based de Core permite y que el precio
no refleja. El porcentaje contra precio en NL baja de ~80 % (oct. 2025–may. 2026) a ~30 % (sept.–oct. 2026),
a la vez que sube la convergencia de precios.

Ojo: son flujos FÍSICOS (A11). Para separar lo comercial ("non-intuitive exchanges") haría falta el
intercambio programado day-ahead de ENTSO-E (A09) o los resultados de Core en JAO. El "valor" es indicativo:
energía física valorada al diferencial de precios, no un coste real.

## JAO Publication Tool — Core (explorado el 19/10/2026; catálogos en `docs/catalogo_jao*.txt`)

API pública, sin token: `https://publicationtool.jao.eu/core/api/data/<publicación>?FromUtc=…&ToUtc=…&Skip=0&Take=…`.
`Take` es obligatorio (máximo 100 millones); se pide el día entero de una vez. Límite: 100 peticiones por minuto.
Desde el Mac hace falta `truststore` (el antivirus o la VPN inspeccionan https), como con ENTSO-E y Elia.

| Dataset bronze | Publicación | Resolución | Volumen por día | Para qué |
|---|---|---|---|---|
| dominio_final | finalComputation, filtro `{"Presolved": true}` | PT60M | 4.000–7.500 filas, ~10 MB (~1 MB en gzip) | RAM, Fmax, FRM, F0, AMR, IVA, objetivo minRAM, PTDFs |
| shadow_prices | shadowPrices | PT15M | ~400–500 | CNECs que limitan el mercado y su shadow price |
| reducciones_validacion | validationReductions | PT60M | ~100–350 | IVA de cada TSO y su justificación |
| intercambios_programados | scheduledExchanges | PT15M | 96 | Intercambio comercial por frontera |
| posicion_neta / posicion_neta_max | netPos / maxNetPos | PT15M / PT60M | 96 / 24 | Posiciones netas |
| maxbex | maxExchanges | PT60M | 24 | Intercambio bilateral máximo |
| alpha | alphaFactor | PT15M | 96 | Peso del dominio FB frente al LTA |
| renta_congestion | congestionIncome | PT15M | 96 | Renta de congestión por zona y TSO |

Notas:
- El dominio completo son ~40.000 filas por hora; con `presolved` quedan ~160–320 CNECs por hora. ELIA es el TSO con más.
- Bronze de JAO se guarda en gzip (`respuesta.json.gz`); el `sha256` del registro es el de la respuesta sin comprimir.
- `tso = 'NA'` son restricciones externas (límites de importación/exportación), no CNECs: se excluyen en gold.
- El TSO viene en mayúsculas en el dominio (`ELIA`) y en mixto en shadow prices (`Elia`): se normaliza a mayúsculas en silver.
- Cumplimiento de minRAM: indicador aproximado RAM/Fmax ≥ `minRamTarget` (que ya incluye derogaciones y planes de acción).
  No es el cálculo oficial de ACER (MACZT con flujos no Core), pero sigue el mismo objetivo publicado por JAO.
- Hueco en la fuente: JAO no publica el alpha factor del 02 al 09/06/2026 (HTTP 200 con 0 filas, comprobado
  en el registro de cargas). El resto de publicaciones de esos días están completas. Test aparte en `warn`.

### Primeros resultados de JAO con el año completo (01/10/2025–12/10/2026)

- **minRAM** (`mart_monthly_min_ram`, % de horas-CNEC con RAM/Fmax ≥ objetivo de JAO): ELIA 99,6 % (RAM media
  0,87·Fmax, AMR en el 12 % de las horas); AMPRION y TENNETBV 99,2 %; en el otro extremo 50HERTZ 80,7 %,
  PSE 82,0 %, TRANSELECTRICA 85,0 %, RTE 86,5 % (RTE necesita AMR en el 65 % de las horas y aplica IVA en el 13,5 %).
- **CNECs que más limitan** (`mart_monthly_active_constraints`, suma de shadow prices): RTE Creys–St-Vulbas 400 kV
  (15.571 cuartos activos), las restricciones externas de NorNed, Baltic Cable y SwePol, y Altheim–Sittling (TenneT DE).
  Para Elia, la restricción externa de importación del ALEGrO (7.810 cuartos). ELIA es el TSO con más cuartos con
  algún CNEC activo (23.069), pero con shadow prices bajos: limita a menudo, con poco coste.
- **Flujo no programado** (`mart_monthly_unscheduled_flow`, físico − comercial, media del año): NL −340 MW
  (entra más de lo programado), FR +310 MW (sale más de lo programado), DE +90 MW. Es un tránsito norte → sur
  de unos 300 MW de media a través de Bélgica; máximo en febrero–marzo de 2026 (−741 y −781 MW en NL) y casi
  nulo en julio–agosto.
- Detalle: algunas restricciones externas llegan sin TSO en shadow prices (p. ej. DE_AL_import): se guardan con tso = ''.
