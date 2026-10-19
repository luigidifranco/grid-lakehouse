-- Por mes y frontera: comercio programado frente a flujo físico, y el flujo no programado entre ambos.
select
    date_trunc('month', market_date)::date                                      as month,
    neighbour,
    count(*)                                                                    as quarters,
    round(avg(commercial_net_export_mw), 1)                                     as avg_commercial_net_export_mw,
    round(avg(physical_net_export_mw), 1)                                       as avg_physical_net_export_mw,
    round(avg(unscheduled_flow_mw), 1)                                          as avg_unscheduled_flow_mw,
    round(avg(abs(unscheduled_flow_mw)), 1)                                     as avg_abs_unscheduled_flow_mw,
    round(100.0 * count(*) filter (where sign(commercial_net_export_mw) <> sign(physical_net_export_mw)
                                     and abs(commercial_net_export_mw) >= {{ var('flow_min_mw') }}
                                     and abs(physical_net_export_mw) >= {{ var('flow_min_mw') }}) / count(*), 1)
                                                                                as pct_opposite_direction
from {{ ref('fct_border_commercial_physical') }}
group by all
