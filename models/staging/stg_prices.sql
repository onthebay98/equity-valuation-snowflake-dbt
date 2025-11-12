with base as (
    select
        upper(symbol) as symbol,
        date::date as date,
        close::float as close,
        volume::float as volume
    from {{ source('raw', 'PRICES') }}
),
latest as (
    select
        symbol,
        max(date) as latest_date
    from base
    group by symbol
)

select
    b.symbol,
    b.date,
    b.close,
    b.volume,
    case when b.date = l.latest_date then 1 else 0 end as is_latest
from base b
join latest l
  on b.symbol = l.symbol
