select
      coalesce(diff_count, 0) as failures,
      coalesce(diff_count, 0) != 0 as should_warn,
      coalesce(diff_count, 0) != 0 as should_error
    from (
      

with  __dbt__cte__stg_AdventureWorks_SalesOrderDetails as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_salesorderdetail`
),

SalesOrderDetails as (
    select
        /* Natural Key */
        salesorderdetailid as salesOrderDetail_id
        ,salesorderid as salesOrder_id
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
),a as (

    select count(*) as count_a from `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`

),
b as (

    select count(*) as count_b from __dbt__cte__stg_AdventureWorks_SalesOrderDetails

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