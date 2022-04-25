with
    /* from Stage */
    SalesOrder as (
        select * from {{ ref('stg_AdventureWorks_SalesOrder') }} 
    ),

    SalesOrderDetail as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderDetail') }}
    ),

    /* from Dimensions */
    dim_CreditCard as (
        select
            customer_sk
            ,customer_id
        from {{ ref('dim_CreditCard') }}
    ),

    dim_Product as (
        select
            employee_sk
            ,employee_id        
        from {{ ref('dim_Product') }}
    ),

    dim_Reasons as (
        select
            product_sk
            ,product_id
        from {{ ref('dim_Reasons') }}
    ),
    
    dim_SalesTerritory as (
        select
            product_sk
            ,product_id
        from {{ ref('dim_SalesTerritory') }}
    ),


    final as (
        select
            /* Surrogate Key */
            row_number() over (order by order_details.order_id, order_details.product_id) as orders_details_sk --auto incremental SK
            /* Natural Key */
            ,orders.order_id
            ,order_details.product_id
            /* Foreing Key */
            ,customer_sk
            ,employee_sk
            ,product_sk
            /* Columns */
            ,order_details.unit_price
            ,order_details.quantity
            ,order_details.discount

            ,order_date 
            ,required_date 
            ,shipped_date 

            ,orders.freight
            ,orders.ship_name
            ,orders.ship_address
            ,orders.ship_city
            ,orders.ship_region
            ,orders.ship_postal_code
            ,orders.ship_country

            ,shippers.company_name as shipper_name
            ,shippers.phone as shipper_phone
        
        from SalesOrderDetail

        left join SalesOrder on SalesOrderDetail.salesOrder_id = SalesOrder.salesOrder_id

        left join dim_CreditCard on orders.customer_id = dim_CreditCard.customer_id
        left join dim_Product on orders.employee_id = dim_Product.employee_id
        left join dim_Reasons on orders.shipper_id = dim_Reasons.shipper_id
        left join dim_SalesTerritory on orders.shipper_id = dim_SalesTerritory.shipper_id
    )

select * from final