with generated_dates as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="2010-01-01",
        end_date="2022-12-30"
    ) }}
),
exploded_dates as (
    select
        sk_date,
        extract(year from sk_date) as year,
        extract(month from sk_date) as month,
        extract(day from sk_date) as day,
        extract(dayofweek from sk_date) as day_of_week,
        extract(week from sk_date) as week,
        extract(quarter from sk_date) as quarter
    from generated_dates
)

final as (
    select 
        sk_date
        , year
        , month
        case 
            when month = 1 then 'Jan'
            when month = 2 then 'Feb'
            when month = 3 then 'Mar'
            when month = 4 then 'Apr'
            when month = 5 then 'May'
            when month = 6 then 'Jun'
            when month = 7 then 'Jul'
            when month = 8 then 'Aug'
            when month = 9 then 'Sep'
            when month = 10 then 'Oct'
            when month = 11 then 'Nov'
            when month = 12 then 'Dec'
        end as month_abbreviation,
        case 
            when day_of_week = 1 then 'Sunday'
            when day_of_week = 2 then 'Monday'
            when day_of_week = 3 then 'Tuesday'
            when day_of_week = 4 then 'Wednesday'
            when day_of_week = 5 then 'Thursday'
            when day_of_week = 6 then 'Friday'
            when day_of_week = 7 then 'Saturday'
        end as weekday,
        quarter
    from generated_dates
)

insert into {{ ref('dim_dates') }} (
    sk_date
    , year
    , month
    , month_abbreviation
    , day_of_week
    , weekday
    , quarter
    , day
)
select
    sk_date
    , year
    , month
    , month_abbreviation
    , day_of_week
    , weekday
    , quarter
    , day
from final