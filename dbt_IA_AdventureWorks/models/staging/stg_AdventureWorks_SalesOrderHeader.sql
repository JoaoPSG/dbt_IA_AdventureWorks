with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesorderheader') }}
),

SalesOrder as (
    select
        /* Natural Key */
        salesorderid
        /* Foreing Key */
        ,salespersonid -- maybe delete
        ,customerid
        ,territoryid
        ,creditcardid
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