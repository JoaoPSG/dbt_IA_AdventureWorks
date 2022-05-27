

  create or replace view `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_ReasonsReference`
  OPTIONS()
  as with
     __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason as (
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
),dim_ReasonsReference as (
        select * from __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason 
    ),

    final as (
        select
            *
        from dim_ReasonsReference
    )

select * from final;

