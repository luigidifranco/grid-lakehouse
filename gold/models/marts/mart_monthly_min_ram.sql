-- Por mes y TSO de Core: cuánta capacidad dejan los CNECs no redundantes frente al objetivo de minRAM,
-- y cuánto se recurre a AMR (ajuste para alcanzar el mínimo) y a IVA (reducción por validación).
select
    date_trunc('month', market_date)::date                                          as month,
    tso,
    count(*)                                                                         as cnec_hours,
    count(distinct cne_name)                                                         as distinct_cnes,
    round(100.0 * count(*) filter (where meets_min_ram_target) / count(*), 2)        as pct_meets_min_ram_target,
    round(avg(ram_ratio), 3)                                                         as avg_ram_ratio,
    round(avg(min_ram_target), 3)                                                    as avg_min_ram_target,
    round(avg(min_ram_factor_pct), 1)                                                as avg_min_ram_factor_pct,
    round(100.0 * count(*) filter (where has_amr) / count(*), 1)                     as pct_with_amr,
    round(avg(amr_mw) filter (where has_amr), 1)                                     as avg_amr_mw,
    round(100.0 * count(*) filter (where has_iva) / count(*), 2)                     as pct_with_iva,
    round(sum(iva_mw), 0)                                                            as total_iva_mw
from {{ ref('fct_cnec_hourly') }}
group by all
