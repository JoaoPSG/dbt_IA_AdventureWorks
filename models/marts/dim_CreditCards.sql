with
    CreditCards as (
        select * from {{ ref('stg_AdventureWorks_CreditCards') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['creditCard_id', 'cardNumber']) }} as creditCard_sk --hashed surrogate key
            /* Natural Key */
            ,creditCard_id
            /* Columms */
            ,cardType
        from CreditCards
    )

select * from final