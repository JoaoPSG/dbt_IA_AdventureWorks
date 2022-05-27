select
      coalesce(diff_count, 0) as failures,
      coalesce(diff_count, 0) != 0 as should_warn,
      coalesce(diff_count, 0) != 0 as should_error
    from (
      

with  __dbt__cte__stg_AdventureWorks_Customers as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_customer`
),

Customers as (
    select
        /* Natural Key */
        customerid as customer_id
        /* Foreing Key */
        ,personid as person_id
        ,territoryid as territory_id
        ,storeid as store_id
        /* Columns */
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from Customers
),a as (

    select count(*) as count_a from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Customers`

),
b as (

    select count(*) as count_b from __dbt__cte__stg_AdventureWorks_Customers

),
final as (

    select
        count_a,
        count_b,
        abs(count_a - count_b) as diff_count
    from a
    cross join b

)

select * from final


      
    ) dbt_internal_test