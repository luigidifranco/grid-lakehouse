-- Flujos físicos ENTSO-E (A11), un sentido por fila. resolution: PT15M o PT60M según lo que publique el TSO.
select out_area, in_area, interval_start_utc, market_date, flow_mw, resolution
from {{ silver('entsoe_physical_flow') }}
