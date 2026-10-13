-- Sistema eléctrico belga por cuarto de hora: consumo, renovables, carga residual, intercambios y precio.
with eolica as (
    select interval_start_utc,
           sum(measured_mw)            as wind_mw,
           sum(forecast_day_ahead_mw)  as wind_forecast_da_mw,
           sum(monitored_capacity_mw)  as wind_capacity_mw
    from {{ ref('stg_wind') }}
    group by all
),
intercambios as (
    select interval_start_utc, sum(net_export_mw_elia) as net_export_mw
    from {{ ref('fct_cross_border_flow') }}
    group by all
)
select
    c.interval_start_utc,
    {{ hora_bruselas('c.interval_start_utc') }}           as interval_start_local,
    c.market_date,
    c.load_mw,
    c.forecast_day_ahead_mw                               as load_forecast_da_mw,
    e.wind_mw,
    e.wind_forecast_da_mw,
    e.wind_capacity_mw,
    s.measured_mw                                         as solar_mw,
    s.forecast_day_ahead_mw                               as solar_forecast_da_mw,
    s.monitored_capacity_mw                               as solar_capacity_mw,
    c.load_mw - coalesce(e.wind_mw, 0) - coalesce(s.measured_mw, 0) as residual_load_mw,
    i.net_export_mw,
    p.price_eur_mwh                                       as price_be_eur_mwh
from {{ ref('stg_total_load') }} c
left join eolica e        using (interval_start_utc)
left join {{ ref('stg_solar') }} s using (interval_start_utc)
left join intercambios i  using (interval_start_utc)
left join {{ ref('stg_day_ahead_price') }} p
    on p.interval_start_utc = c.interval_start_utc and p.bidding_zone = 'BE'
