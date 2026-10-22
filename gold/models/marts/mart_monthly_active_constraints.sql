-- Por mes, CNECs que limitan el acoplamiento de mercado de Core (activos, con shadow price > 0).
-- quarters_active: cuartos de hora en que limitó. shadow_price: €/MW, lo que habría ganado el bienestar
-- social por cada MW más de capacidad en ese CNEC. Ordenando por sum_shadow_price salen los cuellos de botella.
select
    date_trunc('month', market_date)::date                        as month,
    coalesce(nullif(tso, ''), 'UNASSIGNED')                       as tso,   -- restricciones externas sin TSO
    tso = 'ELIA'                                                  as is_elia,
    cnec_name,
    contingency_name,
    direction,
    hub_from,
    hub_to,
    count(*)                                                      as quarters_active,
    round(sum(shadow_price_eur_mw), 0)                            as sum_shadow_price_eur_mw,
    round(avg(shadow_price_eur_mw), 2)                            as avg_shadow_price_eur_mw,
    round(max(shadow_price_eur_mw), 2)                            as max_shadow_price_eur_mw,
    round(avg(ram_mw / nullif(fmax_mw, 0)), 3)                    as avg_ram_ratio
from {{ ref('stg_jao_shadow_price') }}
where shadow_price_eur_mw > 0
group by all
