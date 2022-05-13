

  create or replace table `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`
  
  
  OPTIONS()
  as (
    with
     __dbt__cte__stg_AdventureWorks_SalesReason as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_salesreason`
),

SalesReason as (
    select
        /* Natural Key */
        salesreasonid as salesReason_id
        /* Columns */
        ,"name" as reasonName
        ,reasontype as reassonType
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesReason
),SalesReason as (
        select * from __dbt__cte__stg_AdventureWorks_SalesReason 
    ),

    final as (
        select
            /* Surrogate Key */
            distinct to_hex(md5(cast(coalesce(cast(salesReason_id as 
    string
), '') as 
    string
))) as reason_sk --hashed surrogate key
            /* Natural Key */
            ,salesReason_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesReason
    )

select * from final
  );
  