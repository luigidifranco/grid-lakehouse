-- Flujo neto por frontera desde Bélgica (positivo = exporta), según las dos fuentes, por cuarto de hora.
--
-- Las dos fuentes no siempre publican con la misma resolución: ENTSO-E dio la frontera NL por horas hasta
-- el 29/06/2026 y Elia siempre da cuartos de hora. Para compararlas sin falsas discrepancias, el valor de
-- ENTSO-E se compara con la MEDIA de Elia en el mismo intervalo (una hora o un cuarto). La fila de la hora
-- en punto lleva la comparación; los otros tres cuartos de esa hora no tienen valor de ENTSO-E.
with entsoe as (
    select
        case when out_area = 'BE' then in_area else out_area end      as neighbour,
        interval_start_utc,
        market_date,
        sum(case when out_area = 'BE' then flow_mw else -flow_mw end) as net_export_mw,
        max(resolution)                                                as resolution,
        count(distinct resolution)                                     as n_resolutions
    from {{ ref('stg_physical_flow') }}
    where 'BE' in (out_area, in_area)
    group by all
),
entsoe_intervalos as (
    select *,
           case resolution when 'PT60M' then 4 when 'PT30M' then 2 else 1 end as quarters_covered
    from entsoe
),
-- Media de Elia en cada intervalo de ENTSO-E (para PT15M es el mismo cuarto de hora)
elia_en_intervalo as (
    select e.neighbour, e.interval_start_utc,
           avg(l.net_export_mw)   as net_export_mw,
           count(l.net_export_mw) as quarters_with_elia
    from entsoe_intervalos e
    join {{ ref('stg_border_flow_elia') }} l
      on l.neighbour = e.neighbour
     and l.interval_start_utc >= e.interval_start_utc
     and l.interval_start_utc <  e.interval_start_utc + e.quarters_covered * interval 15 minute
    group by all
)
select
    coalesce(e.neighbour, l.neighbour)                   as neighbour,
    coalesce(e.interval_start_utc, l.interval_start_utc) as interval_start_utc,
    coalesce(e.market_date, l.market_date)               as market_date,
    e.net_export_mw                                      as net_export_mw_entsoe,
    e.resolution                                         as entsoe_resolution,
    e.quarters_covered                                   as entsoe_quarters_covered,
    l.net_export_mw                                      as net_export_mw_elia,
    l.resolution                                         as elia_resolution,
    -- Comparación a la resolución de ENTSO-E (solo si Elia tiene todos los cuartos de ese intervalo)
    case when m.quarters_with_elia = e.quarters_covered then m.net_export_mw end
                                                         as net_export_mw_elia_matched,
    case when m.quarters_with_elia = e.quarters_covered then e.net_export_mw - m.net_export_mw end
                                                         as source_difference_mw
from entsoe_intervalos e
full outer join {{ ref('stg_border_flow_elia') }} l
    on e.neighbour = l.neighbour and e.interval_start_utc = l.interval_start_utc
left join elia_en_intervalo m
    on m.neighbour = e.neighbour and m.interval_start_utc = e.interval_start_utc
