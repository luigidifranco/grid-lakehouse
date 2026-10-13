-- Un día de mercado por fila. quarters_in_day = 92/96/100 según el cambio de hora (hora de Bruselas).
with dias as (
    select distinct market_date from {{ ref('stg_day_ahead_price') }}
    union
    select distinct market_date from {{ ref('stg_total_load') }}
)
select
    market_date,
    year(market_date)                       as year,
    month(market_date)                      as month,
    weekofyear(market_date)                 as iso_week,
    dayname(market_date)                    as day_name,
    isodow(market_date) in (6, 7)           as is_weekend,
    cast(epoch(timezone('Europe/Brussels', (market_date + 1)::timestamp)
             - timezone('Europe/Brussels', market_date::timestamp)) / 900 as integer) as quarters_in_day
from dias
