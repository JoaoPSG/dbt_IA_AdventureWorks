with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesorderdetail') }}
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