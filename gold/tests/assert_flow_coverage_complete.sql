-- Error (bloquea): cada frontera y día debe tener todos sus cuartos de hora en las dos fuentes (92/96/100).
-- Un intervalo horario de ENTSO-E cuenta como 4 cuartos. Detecta los huecos que la comparación no ve.
select market_date, neighbour, quarters_expected, quarters_entsoe, quarters_elia
from {{ ref('mart_daily_flow_reconciliation') }}
where status = 'incomplete'
