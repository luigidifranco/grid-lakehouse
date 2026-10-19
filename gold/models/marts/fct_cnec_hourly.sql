-- CNECs no redundantes del dominio final de Core, por hora, con su margen frente al objetivo de minRAM.
--
-- ram_ratio = RAM / Fmax. JAO publica para cada CNEC min_ram_target (fracción de Fmax que debería quedar
-- disponible para el comercio en Core, según el 70 % del Reglamento 2019/943 y las derogaciones o planes de
-- acción de cada TSO) y min_ram_factor_pct (el porcentaje mínimo aplicable). meets_min_ram_target es un
-- indicador aproximado: compara la RAM publicada con ese objetivo (tolerancia 0,001 por el redondeo de JAO).
select
    interval_start_utc,
    market_date,
    tso,
    tso = 'ELIA'                                             as is_elia,
    cne_name,
    cne_eic,
    direction,
    contingency_name,
    contingency_name = ''                                    as is_base_case,
    element_type,
    hub_from,
    hub_to,
    fmax_mw,
    frm_mw,
    fref_mw,
    f0_core_mw,
    amr_mw,
    iva_mw,
    cva_mw,
    ram_mw,
    min_ram_factor_pct,
    min_ram_target,
    ram_mw / nullif(fmax_mw, 0)                              as ram_ratio,
    ram_mw / nullif(fmax_mw, 0) >= min_ram_target - 0.001    as meets_min_ram_target,
    amr_mw > 0                                               as has_amr,
    iva_mw > 0                                               as has_iva,
    ptdf_BE, ptdf_NL, ptdf_FR, ptdf_DE
from {{ ref('stg_jao_final_domain') }}
where tso <> 'NA'          -- 'NA' son restricciones externas (límites de importación/exportación), no CNECs
  and fmax_mw > 0
