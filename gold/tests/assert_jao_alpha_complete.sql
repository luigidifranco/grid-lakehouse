-- Aviso (no bloquea): días sin alpha factor completo. JAO devolvió respuestas vacías del 02 al 09/06/2026
-- (comprobado en el registro de cargas: HTTP 200, 0 filas). Es un hueco de la fuente, no de la ingesta.
{{ config(severity='warn') }}
with alpha as (
    select market_date, count(*) as n from {{ ref('stg_jao_alpha_factor') }} group by all
)
select d.market_date, d.quarters_in_day, coalesce(a.n, 0) as alpha_quarters
from {{ ref('dim_date') }} d
left join alpha a using (market_date)
where d.market_date between (select min(market_date) from alpha) and (select max(market_date) from alpha)
  and coalesce(a.n, 0) <> d.quarters_in_day
