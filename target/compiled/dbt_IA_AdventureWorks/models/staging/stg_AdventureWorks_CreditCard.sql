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