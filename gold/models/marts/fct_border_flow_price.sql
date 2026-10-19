-- Flujo físico frente a señal de precio, por frontera acoplada y cuarto de hora.
--
-- La electricidad debería ir de la zona barata a la cara. Con flow-based, y por las leyes de Kirchhoff,
-- el flujo FÍSICO no siempre sigue al precio: hay flujos de bucle y de tránsito (p. ej. norte → sur a
-- través de Bélgica). Esto mide cuánto y cuándo; no distingue intercambio comercial de flujo físico.
--
-- flow_vs_price:
--   converged       precios iguales (|diferencial| < price_convergence_eur_mwh): no hay señal de precio
--   not_significant diferencial pequeño (< price_spread_min_eur_mwh) o flujo casi nulo (< flow_min_mw)
--   with_price      el flujo va de la zona barata a la cara
--   against_price   el flujo va de la zona cara a la barata
-- Flujo de Elia (siempre por cuartos de hora y sin huecos); positivo = Bélgica exporta.
with base as (
    select
        f.neighbour,
        b.neighbour_bidding_zone,
        f.interval_start_utc,
        f.market_date,
        be.price_eur_mwh                        as price_be_eur_mwh,
        nb.price_eur_mwh                        as price_neighbour_eur_mwh,
        be.price_eur_mwh - nb.price_eur_mwh     as spread_be_minus_neighbour_eur_mwh,
        f.net_export_mw_elia                    as net_export_mw
    from {{ ref('fct_cross_border_flow') }} f
    join {{ ref('borders') }} b
      on b.neighbour = f.neighbour and b.coupled_day_ahead
    join {{ ref('fct_day_ahead_price') }} be
      on be.bidding_zone = 'BE' and be.interval_start_utc = f.interval_start_utc
    join {{ ref('fct_day_ahead_price') }} nb
      on nb.bidding_zone = b.neighbour_bidding_zone and nb.interval_start_utc = f.interval_start_utc
)
select
    *,
    case
        when abs(spread_be_minus_neighbour_eur_mwh) < {{ var('price_convergence_eur_mwh') }} then 'converged'
        when abs(spread_be_minus_neighbour_eur_mwh) < {{ var('price_spread_min_eur_mwh') }}
          or abs(net_export_mw) < {{ var('flow_min_mw') }}                                then 'not_significant'
        -- exportar (flujo > 0) es ir con el precio si Bélgica es más barata (diferencial < 0), y al revés
        when sign(net_export_mw) = -sign(spread_be_minus_neighbour_eur_mwh)                then 'with_price'
        else 'against_price'
    end                                                          as flow_vs_price,
    -- Energía del cuarto (MWh) valorada al diferencial destino − origen. Negativo = contra el precio.
    -- Indicativo: es flujo físico, no el intercambio comercial que casa el mercado.
    net_export_mw * 0.25 * (-spread_be_minus_neighbour_eur_mwh) as flow_price_value_eur
from base
