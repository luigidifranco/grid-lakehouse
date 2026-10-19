-- Cada frontera acoplada y cada día debe tener todos sus cuartos de hora con precio y flujo (92/96/100).
-- Si falla, falta un precio o un flujo y los porcentajes del mart estarían calculados sobre menos datos.
with conteo as (
    select neighbour, market_date, count(*) as n from {{ ref('fct_border_flow_price') }} group by all
)
select c.*, d.quarters_in_day
from {{ ref('dim_date') }} d
cross join (select neighbour from {{ ref('borders') }} where coupled_day_ahead) b
left join conteo c on c.market_date = d.market_date and c.neighbour = b.neighbour
where coalesce(c.n, 0) <> d.quarters_in_day
