with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_creditcard') }}
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