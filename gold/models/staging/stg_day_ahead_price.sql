-- Precios day-ahead de la subasta principal (SDAC). La secuencia 2 de DE_LU es otra subasta y queda fuera.
select
    bidding_zone,
    interval_start_utc,
    market_date,
    price_eur_mwh
from {{ silver('entsoe_day_ahead_price') }}
where auction_sequence = 1
