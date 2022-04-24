with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_products') }}
),

Customer as (
    select
        /* Natural Key */
        salesorderid
        ,salesorderdetailid
        /* Foreing Key */
        ,productid
        ,specialofferid
        /* Columns */
        ,orderqyt
        ,unitprice
        ,unitpricediscount
        ,carriertrackingnumber
        ,rowguid
        ,modifieddate
    from source
)

select * from Customer