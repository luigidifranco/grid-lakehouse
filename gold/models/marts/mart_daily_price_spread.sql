-- Por día y vecino: diferencial de precio BE − vecino, cuartos con precios iguales (mercados acoplados sin congestión)
-- y flujo neto medio. Muestra si Bélgica importa cuando es más cara, como cabe esperar.
select
    f.market_date,
    b.neighbour,
    b.neighbour_bidding_zone,
    round(avg(be.price_eur_mwh - nb.price_eur_mwh), 2)                          as avg_spread_eur_mwh,
    round(avg(abs(be.price_eur_mwh - nb.price_eur_mwh)), 2)                     as avg_abs_spread_eur_mwh,
    count(*) filter (where abs(be.price_eur_mwh - nb.price_eur_mwh) < 0.01)     as quarters_price_converged,
    count(*)                                                                     as quarters,
    round(avg(f.net_export_mw_elia), 1)                                          as avg_net_export_mw,
    round(corr(be.price_eur_mwh - nb.price_eur_mwh, f.net_export_mw_elia), 2)   as corr_spread_vs_export
from {{ ref('fct_cross_border_flow') }} f
join {{ ref('borders') }} b on b.neighbour = f.neighbour
join {{ ref('fct_day_ahead_price') }} be
    on be.bidding_zone = 'BE' and be.interval_start_utc = f.interval_start_utc
join {{ ref('fct_day_ahead_price') }} nb
    on nb.bidding_zone = b.neighbour_bidding_zone and nb.interval_start_utc = f.interval_start_utc
group by all
