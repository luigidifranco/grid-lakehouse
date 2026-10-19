-- Alpha factor por cuarto de hora: peso del dominio flow-based frente al LTA en el acoplamiento.
select interval_start_utc, market_date, alpha_factor
from {{ silver('jao_alpha_factor') }}
