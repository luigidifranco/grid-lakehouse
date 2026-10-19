-- Intercambio comercial programado tras el acoplamiento day-ahead (JAO), por frontera, sentido y cuarto de hora.
select interval_start_utc, market_date, from_zone, to_zone, mw
from {{ silver('jao_scheduled_exchange') }}
