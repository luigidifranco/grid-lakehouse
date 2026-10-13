-- Eólica: los 5 segmentos (offshore; onshore Flandes/Valonia × Elia/DSO) no se solapan → se pueden sumar
select
    interval_start_utc, market_date, offshore_onshore, region, grid_connection,
    measured_mw, forecast_day_ahead_mw, monitored_capacity_mw
from {{ silver('elia_wind') }}
