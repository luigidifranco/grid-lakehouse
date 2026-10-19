-- Aviso (no bloquea): días y fronteras en que ENTSO-E y Elia discrepan de forma relevante.
-- Una diferencia entre dos publicadores es información para el informe de calidad, no un fallo del pipeline.
{{ config(severity='warn') }}
select market_date, neighbour, comparison_resolution, intervals_over_tolerance, mae_mw, max_abs_difference_mw
from {{ ref('mart_daily_flow_reconciliation') }}
where status = 'relevant_deviation'
