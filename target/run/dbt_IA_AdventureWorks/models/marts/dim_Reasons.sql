

  create or replace view `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`
  OPTIONS()
  as with
     __dbt__cte__stg_AdventureWorks_SalesReasons as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_salesreason`
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
),  __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_salesorderheadersalesreason`
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
),SalesReasons as (
        select * from __dbt__cte__stg_AdventureWorks_SalesReasons 
    ),

    SalesOrderSalesReason as (
        select * from __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason 
    ),

    final as (
        select
            /* Surrogate Key */
            distinct to_hex(md5(cast(coalesce(cast(SalesReasons.salesReason_id as 
    string
), '') || '-' || coalesce(cast(salesOrder_id as 
    string
), '') as 
    string
))) as reason_sk --hashed surrogate key
            /* Natural Key */
            ,SalesReasons.salesReason_id
            /* Foreing Key */
            ,salesOrder_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesOrderSalesReason 
        
        left join SalesReasons on SalesOrderSalesReason.salesReason_id = SalesReasons.salesReason_id
    )

select * from final;

