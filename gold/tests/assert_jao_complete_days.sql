-- Error (bloquea): cada día de mercado debe tener el dominio final completo (23/24/25 horas) y los
-- intercambios programados completos (92/96/100 cuartos). Son la base de los marts de minRAM y de flujo
-- no programado. El alpha factor se comprueba aparte, como aviso: JAO no lo publicó del 02 al 09/06/2026.
with dominio as (
    select market_date, count(distinct interval_start_utc) as n from {{ ref('stg_jao_final_domain') }} group by all
),
intercambios as (
    select market_date, count(distinct interval_start_utc) as n from {{ ref('stg_jao_scheduled_exchange') }} group by all
)
select d.market_date, d.quarters_in_day, i.n as exchange_quarters, o.n as domain_hours
from {{ ref('dim_date') }} d
left join intercambios i using (market_date)
left join dominio o      using (market_date)
where d.market_date between (select min(market_date) from dominio) and (select max(market_date) from dominio)
  and (coalesce(i.n, 0) <> d.quarters_in_day
    or coalesce(o.n, 0) <> d.quarters_in_day / 4)
