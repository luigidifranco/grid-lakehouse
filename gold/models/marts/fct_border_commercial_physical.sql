-- Por frontera de Bélgica y cuarto de hora: intercambio COMERCIAL (lo que casó el mercado, JAO) frente al
-- flujo FÍSICO (lo que pasó por las líneas, Elia). Positivo = Bélgica exporta.
-- La diferencia es flujo no programado: tránsitos y flujos de bucle que atraviesan Bélgica sin ser
-- comercio con ella. LU no está: es parte de la zona DE_LU y no tiene intercambio comercial propio.
with comercial as (
    select
        case when from_zone = 'BE' then to_zone else from_zone end            as neighbour,
        interval_start_utc,
        market_date,
        sum(case when from_zone = 'BE' then mw else -mw end)                   as commercial_net_export_mw
    from {{ ref('stg_jao_scheduled_exchange') }}
    where 'BE' in (from_zone, to_zone)
    group by all
)
select
    c.neighbour,
    c.interval_start_utc,
    c.market_date,
    c.commercial_net_export_mw,
    f.net_export_mw_elia                                                       as physical_net_export_mw,
    f.net_export_mw_elia - c.commercial_net_export_mw                          as unscheduled_flow_mw
from comercial c
join {{ ref('fct_cross_border_flow') }} f
  on f.neighbour = c.neighbour and f.interval_start_utc = c.interval_start_utc
