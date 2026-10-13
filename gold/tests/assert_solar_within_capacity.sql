-- La producción solar medida no puede ser negativa ni superar la capacidad monitorizada (+10 % de margen).
select interval_start_utc, solar_mw, solar_capacity_mw
from {{ ref('fct_be_system') }}
where solar_mw < 0 or solar_mw > 1.1 * solar_capacity_mw
