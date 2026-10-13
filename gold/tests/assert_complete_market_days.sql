-- Cada zona y cada día de mercado deben tener todos sus cuartos de hora (92/96/100). Devuelve los que no.
with conteo as (
    select 'price ' || bidding_zone as serie, market_date, count(*) as n from {{ ref('fct_day_ahead_price') }} group by all
    union all
    select 'load', market_date, count(*) from {{ ref('fct_be_system') }} where load_mw is not null group by all
)
select c.*, d.quarters_in_day
from conteo c join {{ ref('dim_date') }} d using (market_date)
where c.n <> d.quarters_in_day
