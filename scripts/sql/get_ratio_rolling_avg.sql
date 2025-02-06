create table default_liquidation_rolling_avg_hist_{ts}
as
with
get_ratio
as
(
    select
        date_of_repayment,
        number_of_loans,
        "default",
        liquidation,
        case
            when "default" = 0 then null
            else cast(liquidation as decimal) / cast("default" as decimal)
        end as ratio
    from loans
)
, get_wk_rolling_avg
as
(
    select
        date_of_repayment,
        number_of_loans,
        "default",
        liquidation,
        ratio,
        avg(ratio) over (order by date_of_repayment rows between 6 preceding and current row) as ratio_wk_rolling_avg
    from get_ratio
)
select
    date_of_repayment,
    number_of_loans,
    "default",
    liquidation,
    round(ratio, 2) as ratio,
    round(ratio_wk_rolling_avg, 2) as ratio_wk_rolling_avg
from get_wk_rolling_avg
order by date_of_repayment
