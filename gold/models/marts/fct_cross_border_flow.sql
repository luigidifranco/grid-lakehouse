-- Flujo neto por frontera desde Bélgica (positivo = exporta), según las dos fuentes, y su diferencia.
with entsoe as (
    select
        case when out_area = 'BE' then in_area else out_area end as neighbour,
        interval_start_utc,
        market_date,
        sum(case when out_area = 'BE' then flow_mw else -flow_mw end) as net_export_mw
    from {{ ref('stg_physical_flow') }}
    where 'BE' in (out_area, in_area)
    group by all
)
select
    coalesce(e.neighbour, l.neighbour)                   as neighbour,
    coalesce(e.interval_start_utc, l.interval_start_utc) as interval_start_utc,
    coalesce(e.market_date, l.market_date)               as market_date,
    e.net_export_mw                                      as net_export_mw_entsoe,
    l.net_export_mw                                      as net_export_mw_elia,
    e.net_export_mw - l.net_export_mw                    as source_difference_mw
from entsoe e
full outer join {{ ref('stg_border_flow_elia') }} l
    on e.neighbour = l.neighbour and e.interval_start_utc = l.interval_start_utc
