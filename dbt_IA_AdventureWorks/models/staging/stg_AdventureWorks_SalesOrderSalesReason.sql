with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesorderheadersalesreason') }}
),

SalesOrderSalesReason as (
    select
        /* Natural/Foreing Key */
        salesorderid as salesOrder_id
        ,salesreasonid as salesReason_id
        /* Columns */
        ,cast(modifieddate as date) as modifiedDate 
    from source
)

select * from SalesOrderSalesReason