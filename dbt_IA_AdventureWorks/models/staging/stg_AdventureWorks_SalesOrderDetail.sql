with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_products') }}
),

Customer as (
    select
        /* Natural Key */
        salesorderid as salesOrder_id
        ,salesorderdetailid as salesOrderDetail_id
        /* Foreing Key */
        ,productid as product_id
        ,specialofferid as specialOffer_id
        /* Columns */
        ,orderqyt as orderQyt
        ,unitprice as unitPrice
        ,unitpricediscount as unitPriceDiscount
        ,carriertrackingnumber as carrierTrackingNumber
        ,rowguid
        ,cast(modifieddate as date) as modifiedDate
    from source
)

select * from Customer