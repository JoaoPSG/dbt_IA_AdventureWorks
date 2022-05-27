
    
    

with  __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_salesorderheadersalesreason`
),

SalesOrderSalesReason as (
    select
        /* Natural/Foreing Key */
        salesorderid as salesOrder_id
        ,salesreasonid as salesReason_id
        /* Columns */
        ,cast(modifieddate as timestamp) as modifiedDate 
    from source
)

select * from SalesOrderSalesReason
),dbt_test__target as (
  
  select salesOrder_id as unique_field
  from __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason
  where salesOrder_id is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


