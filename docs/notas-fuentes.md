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
