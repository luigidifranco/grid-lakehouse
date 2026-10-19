-- Por mes y frontera acoplada: convergencia de precios y cuánto flujo físico va con o contra el precio.
select
    date_trunc('month', market_date)::date                                         as month,
    neighbour,
    neighbour_bidding_zone,
    count(*)                                                                       as quarters,
    round(100.0 * count(*) filter (where flow_vs_price = 'converged') / count(*), 1) as pct_price_converged,
    round(avg(abs(spread_be_minus_neighbour_eur_mwh)), 2)                          as avg_abs_spread_eur_mwh,
    round(avg(net_export_mw), 1)                                                   as avg_net_export_mw,
    count(*) filter (where flow_vs_price = 'with_price')                           as quarters_with_price,
    count(*) filter (where flow_vs_price = 'against_price')                        as quarters_against_price,
    round(100.0 * count(*) filter (where flow_vs_price = 'against_price')
          / nullif(count(*) filter (where flow_vs_price in ('with_price', 'against_price')), 0), 1)
                                                                                   as pct_against_price,
    round(sum(abs(net_export_mw) * 0.25) filter (where flow_vs_price = 'against_price'))
                                                                                   as energy_against_price_mwh,
    round(sum(flow_price_value_eur) filter (where flow_vs_price = 'against_price')) as value_against_price_eur,
    round(sum(flow_price_value_eur))                                               as flow_price_value_eur
from {{ ref('fct_border_flow_price') }}
group by all
