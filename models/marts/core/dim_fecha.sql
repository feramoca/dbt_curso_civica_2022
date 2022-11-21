-- crea la tabla dimension fechas

{{
  config(
    materialized='table'
  )
}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2021-01-01' as date)",
    end_date="cast('2023-01-01' as date)"
   )
}}