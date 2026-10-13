-- Solar: solo la fila de país. Regiones y provincias son desgloses del mismo total (sumarlas lo triplicaría).
select
    interval_start_utc, market_date, measured_mw, forecast_day_ahead_mw, monitored_capacity_mw
from {{ silver('elia_solar') }}
where region_level = 'country'
