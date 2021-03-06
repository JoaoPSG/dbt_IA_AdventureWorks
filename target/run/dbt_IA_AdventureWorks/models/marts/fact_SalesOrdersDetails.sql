

  create or replace view `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`
  OPTIONS()
  as with
     __dbt__cte__stg_AdventureWorks_SalesOrders as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_salesorderheader`
),

SalesOrders as (
    select
        /* Natural Key */
        salesorderid as salesOrder_id
        /* Foreing Key */
        --,salespersonid -- maybe delete
        ,customerid as customer_id
        --,territoryid as territory_id
        ,creditcardid as creditCard_id
        ,billtoaddressid as billToAddress_id
        /* Columns */
        ,source.status as orderStatus
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
),  __dbt__cte__stg_AdventureWorks_SalesOrderDetails as (
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
),/* from Stage */
    SalesOrders as (
        select * from __dbt__cte__stg_AdventureWorks_SalesOrders 
    ),

    SalesOrderDetails as (
        select * from __dbt__cte__stg_AdventureWorks_SalesOrderDetails
    ),

    SalesOrderSalesReason as (
        select * from __dbt__cte__stg_AdventureWorks_SalesOrderSalesReason 
    ),

    /* from Dimensions */
    dim_Addresses as (
        select
            address_sk
            ,address_id
        from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Addresses`
    ),


    dim_CreditCards as (
        select
            creditCard_sk
            ,creditCard_id
        from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_CreditCards`
    ),

    dim_Products as (
        select
            product_sk
            ,product_id        
        from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Products`
    ),

    dim_Customers as (
        select
            customer_sk
            ,customer_id
        from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Customers`
    ),

    -- dim_Reasons as (
    --     select
    --         reason_sk
    --         ,salesReason_id
    --     from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`
    -- ),


    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(SalesOrders.salesOrder_id as 
    string
), '') || '-' || coalesce(cast(SalesOrderDetails.salesOrderDetail_id as 
    string
), '') as 
    string
))) as salesOrdersDetais_sk --hashed surrogate key
            /* Natural Key */
            ,SalesOrders.salesOrder_id
            ,SalesOrderDetails.salesOrderDetail_id
            /* Foreing Key */
            ,creditCard_sk
            ,product_sk
            -- ,reason_sk
            -- ,territory_sk
            ,customer_sk
            ,billToAddress_id
            /* Columns */
            ,orderQty
            ,unitPrice
            ,unitPriceDiscount
            ,SalesOrders.orderDate
            ,SalesOrders.orderStatus

            -- ,carrierTrackingNumber
            -- ,SalesOrders.taxamt
            -- ,SalesOrders.comment
            ,SalesOrders.dueDate
            ,SalesOrders.shipDate
            -- ,SalesOrders.freight
            -- ,SalesOrders.subtotal
            -- ,SalesOrders.totalDue
            -- ,SalesOrders.accountNumber
            -- ,SalesOrders.revisionNumber
            ,SalesOrders.onlineOrderFlag
            -- ,SalesOrders.purchaseOrderNumber
            -- ,SalesOrders.creditCardApprovalCode
            --,SalesOrders.rowguid
            --,SalesOrders.modifiedDate
        from SalesOrderDetails

        left join SalesOrders on SalesOrderDetails.salesOrder_id = SalesOrders.salesOrder_id

        left join dim_CreditCards on SalesOrders.creditCard_id = dim_CreditCards.creditCard_id
        left join dim_Products on SalesOrderDetails.product_id = dim_Products.product_id
        left join dim_Customers on SalesOrders.customer_id = dim_Customers.customer_id
        
        -- left join SalesOrderSalesReason on SalesOrders.salesOrder_id = SalesOrderSalesReason.salesOrder_id
        -- left join dim_Reasons on SalesOrderSalesReason.salesReason_id = dim_Reasons.salesReason_id
    )

select * from final;

