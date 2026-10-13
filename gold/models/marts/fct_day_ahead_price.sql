select
    p.bidding_zone,
    p.interval_start_utc,
    {{ hora_bruselas('p.interval_start_utc') }} as interval_start_local,
    p.market_date,
    p.price_eur_mwh
from {{ ref('stg_day_ahead_price') }} p
