-- ENTSO-E y Elia deben dar el mismo flujo neto por frontera (tolerancia 1 MW). Devuelve los cuartos que no cuadran.
select * from {{ ref('fct_cross_border_flow') }}
where abs(source_difference_mw) > 1
