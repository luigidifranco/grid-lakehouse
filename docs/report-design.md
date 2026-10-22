# Power BI report — design

Semantic model in **Direct Lake** mode on the Delta tables exported by `serve/export_gold.py`
(`data/serving/` → Lakehouse `Tables/`). The report is built in the Power BI web editor
(Power BI Desktop does not run on macOS).

## Model

Star schema. All facts and marts relate to the dimensions through single-direction, many-to-one relationships.

| From (many) | Column | To (one) | Column |
|---|---|---|---|
| fct_day_ahead_price, fct_be_system, fct_cross_border_flow, fct_border_flow_price, fct_border_commercial_physical, fct_cnec_hourly, mart_daily_* | market_date | dim_date | market_date |
| mart_monthly_* | month | dim_date | market_date (first day of the month) |
| fct_cross_border_flow, fct_border_flow_price, fct_border_commercial_physical, mart_daily_flow_reconciliation, mart_daily_price_spread, mart_monthly_border_flow_price, mart_monthly_unscheduled_flow | neighbour | dim_border | neighbour |
| fct_day_ahead_price | bidding_zone | dim_bidding_zone | bidding_zone |
| fct_cnec_hourly, mart_monthly_min_ram, mart_monthly_active_constraints | tso | dim_tso | tso |

Time: `interval_start_utc` is the true instant; `interval_start_local`, `hour_local` and `quarter_of_day`
are Brussels wall-clock values for hour-of-day visuals (clock-change days have 92 or 100 quarter-hours).
Mark `dim_date` as the date table.

## Measures (DAX)

```dax
-- Data quality
Days reconciled ok % =
DIVIDE ( CALCULATE ( COUNTROWS ( mart_daily_flow_reconciliation ), mart_daily_flow_reconciliation[status] = "ok" ),
         COUNTROWS ( mart_daily_flow_reconciliation ) )
Relevant deviation days = CALCULATE ( COUNTROWS ( mart_daily_flow_reconciliation ),
                                      mart_daily_flow_reconciliation[status] = "relevant_deviation" )
Flow MAE (MW) = AVERAGEX ( mart_daily_flow_reconciliation, mart_daily_flow_reconciliation[mae_mw] )

-- Prices and flows
Avg BE price (€/MWh) = CALCULATE ( AVERAGE ( fct_day_ahead_price[price_eur_mwh] ), dim_bidding_zone[bidding_zone] = "BE" )
Price convergence % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_border_flow_price ), fct_border_flow_price[flow_vs_price] = "converged" ),
                               COUNTROWS ( fct_border_flow_price ) )
Against-price share % =
VAR WithSignal = CALCULATE ( COUNTROWS ( fct_border_flow_price ), fct_border_flow_price[flow_vs_price] IN { "with_price", "against_price" } )
RETURN DIVIDE ( CALCULATE ( COUNTROWS ( fct_border_flow_price ), fct_border_flow_price[flow_vs_price] = "against_price" ), WithSignal )
Energy against price (GWh) = DIVIDE ( CALCULATE ( SUMX ( fct_border_flow_price, ABS ( fct_border_flow_price[net_export_mw] ) * 0.25 ),
                                                  fct_border_flow_price[flow_vs_price] = "against_price" ), 1000 )

-- Commercial vs physical
Avg commercial net export (MW) = AVERAGE ( fct_border_commercial_physical[commercial_net_export_mw] )
Avg physical net export (MW)   = AVERAGE ( fct_border_commercial_physical[physical_net_export_mw] )
Avg unscheduled flow (MW)      = AVERAGE ( fct_border_commercial_physical[unscheduled_flow_mw] )

-- Core flow-based (JAO)
minRAM target met % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_cnec_hourly ), fct_cnec_hourly[meets_min_ram_target] = TRUE () ),
                               COUNTROWS ( fct_cnec_hourly ) )
Avg RAM / Fmax = AVERAGE ( fct_cnec_hourly[ram_ratio] )
AMR use % = DIVIDE ( CALCULATE ( COUNTROWS ( fct_cnec_hourly ), fct_cnec_hourly[has_amr] = TRUE () ), COUNTROWS ( fct_cnec_hourly ) )
Binding quarter-hours = SUM ( mart_monthly_active_constraints[quarters_active] )
Sum of shadow prices (€/MW) = SUM ( mart_monthly_active_constraints[sum_shadow_price_eur_mw] )
```

## Pages

1. **Overview** — cards: days loaded, sources (ENTSO-E, Elia, JAO), automated tests, avg BE price,
   price convergence %, Elia minRAM target met %, avg transit through Belgium (MW). One line chart of the BE price.
2. **Data quality** — matrix border × month coloured by reconciliation status; flow MAE by month; annotation of the
   NL hourly → quarter-hourly change at ENTSO-E (30/06/2026) and the 7-day revision window; table of relevant days.
3. **Prices and flows** — price convergence % by border and month; 100 % stacked bar of with/against/converged/not
   significant per border; scatter of spread vs net export per border (sampled to one point per hour).
4. **Commercial vs physical** — monthly clustered columns commercial vs physical net export for NL, FR, DE; line of
   unscheduled flow; hour-of-day profile of unscheduled flow on BE–NL and BE–FR (the north–south transit).
5. **Core grid constraints** — bar of minRAM target met % by TSO (Elia highlighted); Avg RAM/Fmax vs target per TSO;
   AMR use % by TSO; top 15 binding CNECs by sum of shadow prices, with slicer for Elia only.
6. **Belgian system** — load, wind, solar and residual load by hour of day; day-ahead forecast MAE and bias by month.

Every page has a footnote with the source and, where needed, the caveat: physical flows are not commercial
exchanges; the minRAM indicator is RAM/Fmax against JAO's published target, not ACER's official MACZT figure.
