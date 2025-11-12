with base as (
    select
        upper(symbol) as symbol,
        fiscal_year::int as fiscal_year,
        revenue::float as revenue,
        ebitda::float as ebitda,
        net_income::float as net_income,
        fcf::float as fcf,
        total_debt::float as total_debt,
        cash::float as cash,
        shares_out::float as shares_out,
        dividends::float as dividends
    from {{ source('raw', 'FUNDAMENTALS') }}
),
latest as (
    select
        symbol,
        max(fiscal_year) as latest_year
    from base
    group by symbol
)

select b.*
from base b
join latest l
  on b.symbol = l.symbol
 and b.fiscal_year = l.latest_year
