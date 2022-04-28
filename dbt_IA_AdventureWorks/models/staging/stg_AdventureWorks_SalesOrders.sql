with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesorderheader') }}
),

SalesOrders as (
    select
        /* Natural Key */
        salesorderid as salesOrder_id
        /* Foreing Key */
        ,salespersonid -- maybe delete
        ,customerid as customer_id
        ,territoryid as territory_id
        ,creditcardid as creditCard_id
        /* Columns */
        ,"status" as orderStatus
        ,taxamt
        ,comment
        ,duedate as dueDate
        ,freight
        ,cast(shipdate as timestamp) as shipDate
        ,subtotal
        ,totaldue as totalDue
        ,cast(orderdate as timestamp) as orderDate
        ,accountnumber as accountNumber
        ,revisionnumber as revisionNumber
        ,onlineorderflag as onlineOrderFlag
        ,purchaseordernumber as purchaseOrderNumber
        ,creditcardapprovalcode as creditCardApprovalCode
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesOrders