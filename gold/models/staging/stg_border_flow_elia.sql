-- Flujo por frontera según Elia. Convenio: positivo = Bélgica exporta, negativo = importa.
select neighbour, interval_start_utc, market_date, flow_mw as net_export_mw
from {{ silver('elia_border_flow') }}
