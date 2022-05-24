select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with  __dbt__cte__stg_AdventureWorks_SalesOrderDetails as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_salesorderdetail`
),

SalesOrderDetails as (
    select
        /* Natural Key */
        salesorderid as salesOrder_id
        ,salesorderdetailid as salesOrderDetail_id
        /* Foreing Key */
        ,productid as product_id
        ,specialofferid as specialOffer_id
        /* Columns */
        ,orderqty as orderQty
        ,unitprice as unitPrice
        ,unitpricediscount as unitPriceDiscount
        ,carriertrackingnumber as carrierTrackingNumber
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesOrderDetails
),dbt_test__target as (
  
  select salesOrderDetail_id as unique_field
  from __dbt__cte__stg_AdventureWorks_SalesOrderDetails
  where salesOrderDetail_id is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



      
    ) dbt_internal_test