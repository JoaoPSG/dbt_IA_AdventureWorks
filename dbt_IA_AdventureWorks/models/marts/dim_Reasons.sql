with
    SalesReason as (
        select * from {{ ref('stg_AdventureWorks_SalesReason') }} 
    ),

    SalesOrderSalesReason as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderSalesReason') }} 
    ),


    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['SalesOrderSalesReason.salesReason_id']) }} as reason_sk --hashed surrogate key
            /* Natural Key */
            ,SalesOrderSalesReason.salesOrder_id
            ,SalesOrderSalesReason.salesReason_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesOrderSalesReason

        left join SalesReason on SalesOrderSalesReason.salesReason_id = SalesReason.salesReason_id 
    )

select * from final