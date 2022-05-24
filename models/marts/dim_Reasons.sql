with
    SalesReasons as (
        select * from {{ ref('stg_AdventureWorks_SalesReasons') }} 
    ),

    SalesOrderSalesReason as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderSalesReason') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            distinct {{ dbt_utils.surrogate_key(['SalesReasons.salesReason_id']) }} as reason_sk --hashed surrogate key
            /* Natural Key */
            ,SalesReasons.salesReason_id
            /* Foreing Key */
            salesOrder_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesReasons

        left join SalesOrderSalesReason on SalesReasons.salesReason_id = SalesOrderSalesReason.salesReason_id
    )

select * from final