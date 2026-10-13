select
    interval_start_utc, market_date, load_mw,
    forecast_day_ahead_mw, forecast_day_ahead_p10_mw, forecast_day_ahead_p90_mw,
    forecast_most_recent_mw, forecast_week_ahead_mw
from {{ silver('elia_total_load') }}
