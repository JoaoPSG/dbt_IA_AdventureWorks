with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_creditcard') }}
),

CreditCards as (
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

select * from CreditCards