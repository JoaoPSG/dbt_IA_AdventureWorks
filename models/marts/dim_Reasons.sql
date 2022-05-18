with
    SalesReasons as (
        select * from {{ ref('stg_AdventureWorks_SalesReasons') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            distinct {{ dbt_utils.surrogate_key(['salesReason_id']) }} as reason_sk --hashed surrogate key
            /* Natural Key */
            ,salesReason_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesReasons
    )

select * from final