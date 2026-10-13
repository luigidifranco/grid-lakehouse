-- Error de la previsión day-ahead de Elia por día: MAE, sesgo (+ = se quedó corta) y MAPE del consumo.
with base as (
    select market_date, 'load' as metric, load_mw as actual_mw, load_forecast_da_mw as forecast_mw from {{ ref('fct_be_system') }}
    union all
    select market_date, 'wind', wind_mw, wind_forecast_da_mw from {{ ref('fct_be_system') }}
    union all
    select market_date, 'solar', solar_mw, solar_forecast_da_mw from {{ ref('fct_be_system') }}
)
select
    market_date,
    metric,
    count(*)                                         as quarters,
    round(avg(abs(actual_mw - forecast_mw)), 1)      as mae_mw,
    round(avg(actual_mw - forecast_mw), 1)           as bias_mw,
    case when metric = 'load'
         then round(100 * avg(abs(actual_mw - forecast_mw) / nullif(actual_mw, 0)), 2) end as mape_pct
from base
where actual_mw is not null and forecast_mw is not null
group by market_date, metric
