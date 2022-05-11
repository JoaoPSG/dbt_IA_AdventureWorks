with
     __dbt__cte__stg_AdventureWorks_CreditCard as (
with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_creditcard`
),

CreditCard as (
    select
        /* Natural Key */
        creditcardid as creditCard_id
        /* Columms */
        ,cardnumber as cardNumber
        ,cardtype as cardType
        ,expyear as expYear
        ,expmonth as expMonth
        ,modifieddate as modifiedDate
    from source
)

select * from CreditCard
),CreditCard as (
        select * from __dbt__cte__stg_AdventureWorks_CreditCard 
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(creditCard_id as 
    string
), '') as 
    string
))) as creditCard_sk --hashed surrogate key
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