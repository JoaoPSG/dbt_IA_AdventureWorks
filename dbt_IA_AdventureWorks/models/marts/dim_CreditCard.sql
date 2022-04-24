with
    CreditCard as (
        select * from {{ ref('stg_AdventureWorks_CreditCard') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            row_number() over (order by creditCard_id) as Reason_sk --auto incremental surrogate key
            /* Natural Key */
            ,creditCard_id
            /* Columms */
            ,cardNumber
            ,cardType
            ,expYear
            ,expMonth
        from CreditCard
    )

select * from final