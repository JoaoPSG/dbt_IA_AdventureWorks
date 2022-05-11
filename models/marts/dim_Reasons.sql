with
    SalesReason as (
        select * from {{ ref('stg_AdventureWorks_SalesReason') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            distinct {{ dbt_utils.surrogate_key(['salesReason_id', 'reasonName', 'reassonType']) }} as reason_sk --hashed surrogate key
            /* Natural Key */
            ,salesReason_id
            /* Columms */
            ,reasonName
            ,reassonType
        from SalesReason
    )

select * from final