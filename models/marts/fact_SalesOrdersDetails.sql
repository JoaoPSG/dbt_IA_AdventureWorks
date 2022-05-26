with
    /* from Stage */
    SalesOrders as (
        select * from {{ ref('stg_AdventureWorks_SalesOrders') }} 
    ),

    SalesOrderDetails as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderDetails') }}
    ),

    SalesOrderSalesReason as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderSalesReason') }} 
    ),

    /* from Dimensions */
    dim_Addresses as (
        select
            address_sk
            ,address_id
        from {{ ref('dim_Addresses') }}
    ),


    dim_CreditCards as (
        select
            creditCard_sk
            ,creditCard_id
        from {{ ref('dim_CreditCards') }}
    ),

    dim_Products as (
        select
            product_sk
            ,product_id        
        from {{ ref('dim_Products') }}
    ),

    dim_Customers as (
        select
            customer_sk
            ,customer_id
        from {{ ref('dim_Customers') }}
    ),

    dim_Reasons as (
        select
            reason_sk
            ,salesReason_id
        from {{ ref('dim_Reasons') }}
    ),


    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['SalesOrders.salesOrder_id', 'SalesOrderDetails.salesOrderDetail_id']) }} as salesOrdersDetais_sk --hashed surrogate key
            /* Natural Key */
            ,SalesOrders.salesOrder_id
            ,SalesOrderDetails.salesOrderDetail_id
            /* Foreing Key */
            ,creditCard_sk
            ,product_sk
            ,reason_sk
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
        
        left join SalesOrderSalesReason on SalesOrders.salesOrder_id = SalesOrderSalesReason.salesOrder_id
        left join dim_Reasons on SalesOrderSalesReason.salesReason_id = dim_Reasons.salesReason_id
    )

select * from final