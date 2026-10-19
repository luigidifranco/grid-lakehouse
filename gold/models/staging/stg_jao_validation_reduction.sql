-- Reducciones de capacidad por validación (IVA) de cada TSO de Core, con su justificación.
select * exclude (source_file, loaded_at)
from {{ silver('jao_validation_reduction') }}
