-- Dominio flow-based final de Core (JAO), solo CNECs no redundantes (presolved). Un registro por CNEC y hora.
select * exclude (source_file, loaded_at)
from {{ silver('jao_final_domain') }}
