with
    /* from Stage */
    SalesOrders as (
        select * from {{ ref('stg_AdventureWorks_SalesOrders') }} 
    ),

    SalesOrderDetails as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderDetails') }}
    ),

    /* from Dimensions */
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

    dim_Reasons as (
        select
            reason_sk
            ,salesOrder_id
        from {{ ref('dim_Reasons') }}
    ),
    
    dim_SalesTerritories as (
        select
            territory_sk
            ,territory_id
        from {{ ref('dim_SalesTerritories') }}
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
            ,territory_sk
            /* Columns */
            ,SalesOrders.orderStatus
            ,SalesOrders.taxamt
            ,SalesOrders.comment
            ,SalesOrders.dueDate
            ,SalesOrders.freight
            ,SalesOrders.shipDate
            ,SalesOrders.subtotal
            ,SalesOrders.totalDue
            ,SalesOrders.orderDate
            ,SalesOrders.accountNumber
            ,SalesOrders.revisionNumber
            ,SalesOrders.onlineOrderFlag
            ,SalesOrders.purchaseOrderNumber
            ,SalesOrders.creditCardApprovalCode
            ,SalesOrders.rowguid
            ,SalesOrders.modifiedDate
        from SalesOrderDetails

        left join SalesOrders on SalesOrderDetails.salesOrder_id = SalesOrders.salesOrder_id

        left join dim_CreditCards on SalesOrders.creditCard_id = dim_CreditCards.creditCard_id
        left join dim_Products on SalesOrderDetails.product_id = dim_Products.product_id
        left join dim_Reasons on SalesOrders.salesOrder_id = dim_Reasons.salesOrder_id
        left join dim_SalesTerritories on SalesOrders.territory_id = dim_SalesTerritories.territory_id
    )

select * from final