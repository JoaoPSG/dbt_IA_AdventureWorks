with
    dim_ReasonsReference as (
        select * from {{ ref('stg_AdventureWorks_SalesOrderSalesReason') }} 
    ),

    final as (
        select
            *
        from dim_ReasonsReference
    )

select * from final