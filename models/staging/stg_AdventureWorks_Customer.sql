with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_customer') }}
),

Customer as (
    select
        /* Natural Key */
        customerid as customer_id
        /* Foreing Key */
        ,personid as person_id
        ,territoryid as territory_id
        -- ,storeid as store_id
        /* Columns */
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from Customer