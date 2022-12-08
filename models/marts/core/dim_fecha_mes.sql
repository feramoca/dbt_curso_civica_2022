

with date as (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date()+31 as date)"
    )
    }}  
)

select
      date_month as fecha
    , year(date_month) as anio
    , month(date_month) as mes
    , monthname(date_month) as desc_mes
    , year(date_month)*100+month(date_month) as id_anio_mes
    , month (date_month-1) as mes_previo

from date
