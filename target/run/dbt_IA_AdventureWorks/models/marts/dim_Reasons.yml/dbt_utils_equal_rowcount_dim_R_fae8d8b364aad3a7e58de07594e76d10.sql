select
      coalesce(diff_count, 0) as failures,
      coalesce(diff_count, 0) != 0 as should_warn,
      coalesce(diff_count, 0) != 0 as should_error
    from (
      

with  __dbt__cte__stg_AdventureWorks_SalesReasons as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_salesreason`
),

SalesReason as (
    select
        /* Natural Key */
        salesreasonid as salesReason_id
        /* Columns */
        ,source.name as reasonName
        ,reasontype as reassonType
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesReason
),a as (

    select count(*) as count_a from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`

),
b as (

    select count(*) as count_b from __dbt__cte__stg_AdventureWorks_SalesReasons

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