-- Calidad de datos entre fuentes: por día de mercado y frontera, ¿cuadran los flujos de ENTSO-E y de Elia?
--
-- status, por orden de prioridad:
--   incomplete          falta algún cuarto de hora en una de las dos fuentes
--   provisional         día dentro de la ventana de revisión: las fuentes aún pueden corregirlo
--   relevant_deviation  algún intervalo supera flow_relevant_max_mw o la media diaria supera flow_relevant_mae_mw
--   minor_deviation     hay diferencias por encima de la tolerancia, pero pequeñas
--   ok                  todo dentro de la tolerancia
-- comparison_resolution dice a qué resolución se ha comparado (PT60M cuando ENTSO-E publicaba por horas).
with por_dia as (
    select
        f.market_date,
        f.neighbour,
        string_agg(distinct f.entsoe_resolution, ',' order by f.entsoe_resolution)  as comparison_resolution,
        string_agg(distinct f.elia_resolution, ',' order by f.elia_resolution)      as elia_resolution,
        count(f.net_export_mw_elia)                                                  as quarters_elia,
        coalesce(sum(f.entsoe_quarters_covered), 0)                                  as quarters_entsoe,
        count(f.source_difference_mw)                                                as intervals_compared,
        count(*) filter (where abs(f.source_difference_mw) > {{ var('flow_tolerance_mw') }})
                                                                                     as intervals_over_tolerance,
        avg(abs(f.source_difference_mw))                                             as mae_mw,
        avg(f.source_difference_mw)                                                  as bias_entsoe_minus_elia_mw,
        max(abs(f.source_difference_mw))                                             as max_abs_difference_mw
    from {{ ref('fct_cross_border_flow') }} f
    group by all
)
select
    p.market_date,
    p.neighbour,
    d.quarters_in_day                                    as quarters_expected,
    p.quarters_entsoe,
    p.quarters_elia,
    p.comparison_resolution,
    p.elia_resolution,
    p.intervals_compared,
    p.intervals_over_tolerance,
    round(p.mae_mw, 2)                                   as mae_mw,
    round(p.bias_entsoe_minus_elia_mw, 2)                as bias_entsoe_minus_elia_mw,
    round(p.max_abs_difference_mw, 2)                    as max_abs_difference_mw,
    case
        when p.quarters_entsoe <> d.quarters_in_day or p.quarters_elia <> d.quarters_in_day
            then 'incomplete'
        when p.market_date > current_date - {{ var('revision_days') }}
            then 'provisional'
        when p.max_abs_difference_mw > {{ var('flow_relevant_max_mw') }}
          or p.mae_mw > {{ var('flow_relevant_mae_mw') }}
            then 'relevant_deviation'
        when p.intervals_over_tolerance > 0
            then 'minor_deviation'
        else 'ok'
    end                                                  as status
from por_dia p
join {{ ref('dim_date') }} d using (market_date)
