-- Flujos físicos ENTSO-E (A11), un sentido por fila
select out_area, in_area, interval_start_utc, market_date, flow_mw
from {{ silver('entsoe_physical_flow') }}
