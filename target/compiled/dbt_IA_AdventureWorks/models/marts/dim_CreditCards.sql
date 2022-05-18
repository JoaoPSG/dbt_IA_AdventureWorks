with
     __dbt__cte__stg_AdventureWorks_CreditCards as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_creditcard`
),

CreditCards as (
    select
        /* Natural Key */
        creditcardid as creditCard_id
        /* Columms */
        ,cardnumber as cardNumber --sensible data
        ,cardtype as cardType
        --,expyear as expYear --sensible data
        --,expmonth as expMonth --sensible data
        ,modifieddate as modifiedDate
    from source
)

select * from CreditCards
),CreditCards as (
        select * from __dbt__cte__stg_AdventureWorks_CreditCards 
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(creditCard_id as 
    string
), '') || '-' || coalesce(cast(cardNumber as 
    string
), '') as 
    string
))) as creditCard_sk --hashed surrogate key
            /* Natural Key */
            ,creditCard_id
            /* Columms */
            ,cardType
        from CreditCards
    )

select * from final