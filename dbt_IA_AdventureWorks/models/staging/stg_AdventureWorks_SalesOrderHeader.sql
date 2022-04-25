with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesorderheader') }}
),

SalesOrder as (
    select
        /* Natural Key */
        salesorderid as salesOrder_id
        /* Foreing Key */
        ,salespersonid -- maybe delete
        ,customerid as customer_id
        ,territoryid as territory_id
        ,creditcardid as creditCard_id
        /* Columns */
        ,"status"
        ,taxamt
        ,comment
        ,duedate
        ,freight
        ,cast(shipdate as date) as shipDate
        ,subtotal
        ,totaldue
        ,orderdate
        ,accountnumber
        ,revisionnumber
        ,onlineorderflag
        ,purcharseordernumber
        ,creditcardapprovalcode
        ,rowguid
        ,cast(modifieddate as date) as modifiedDate
    from source
)

select * from SalesOrders