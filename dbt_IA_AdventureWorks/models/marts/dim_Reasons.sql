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
            row_number() over (order by salesReason_id) as Reason_sk --auto incremental surrogate key
            /* Natural Key */
            ,SalesOrderSalesReason.salesorderid
            ,SalesOrderSalesReason.salesreasonid
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesOrderSalesReason

        left join SalesReason on SalesOrderSalesReason.salesReason_id = SalesReason.salesReason_id 
    )

select * from final