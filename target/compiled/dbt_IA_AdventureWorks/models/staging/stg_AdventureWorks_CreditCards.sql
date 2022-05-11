with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_creditcard`
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