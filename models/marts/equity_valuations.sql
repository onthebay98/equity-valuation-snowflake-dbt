with prices as (
    select symbol, close as price
    from {{ ref('stg_prices') }}
    where is_latest = 1
),
f as (
    select *
    from {{ ref('stg_fundamentals') }}
)

select
    p.symbol,
    p.price,
    f.fiscal_year,
    f.revenue,
    f.ebitda,
    f.net_income,
    f.fcf,
    f.total_debt,
    f.cash,
    f.shares_out,
    f.dividends,

    /* Market cap & EV */
    (p.price * f.shares_out) as market_cap,
    (p.price * f.shares_out) + f.total_debt - f.cash as enterprise_value,

    /* Multiples */
    case when f.net_income > 0
        then (p.price * f.shares_out) / f.net_income
    end as pe,

    case when f.ebitda > 0
        then ((p.price * f.shares_out) + f.total_debt - f.cash) / f.ebitda
    end as ev_ebitda,

    case when f.revenue > 0
        then ((p.price * f.shares_out) + f.total_debt - f.cash) / f.revenue
    end as ev_sales,

    case when (p.price * f.shares_out) > 0
        then f.fcf / (p.price * f.shares_out)
    end as fcf_yield,

    case when (p.price * f.shares_out) > 0
        then f.dividends / (p.price * f.shares_out)
    end as div_yield,

    /* Simple intrinsic value: normalize FCF yield at 6% */
    case
        when f.fcf > 0 and f.shares_out > 0
            then (f.fcf / 0.06) / f.shares_out
    end as iv_fcf_yield_6pct,

    case
        when f.fcf > 0 and f.shares_out > 0 and p.price > 0
            then ((f.fcf / 0.06) / f.shares_out - p.price) / p.price
    end as iv_fcf_yield_6pct_upside

from prices p
join f
  on p.symbol = f.symbol
order by iv_fcf_yield_6pct_upside desc nulls last
