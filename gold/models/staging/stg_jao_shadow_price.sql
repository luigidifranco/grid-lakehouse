-- CNECs activos tras el acoplamiento de mercado de Core (JAO), por cuarto de hora, con su shadow price (€/MW).
select * exclude (source_file, loaded_at)
from {{ silver('jao_shadow_price') }}
