with
    CreditCards as (
        select * from {{ ref('stg_AdventureWorks_CreditCards') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['creditCard_id']) }} as creditCard_sk --hashed surrogate key
            /* Natural Key */
            ,creditCard_id
            /* Columms */
            ,cardNumber
            ,cardType
            ,expYear
            ,expMonth
        from CreditCards
    )

select * from final