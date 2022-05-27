with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_customer`
),

Customers as (
    select
        /* Natural Key */
        customerid as customer_id
        /* Foreing Key */
        ,personid as person_id
        ,territoryid as territory_id
        ,storeid as store_id
        /* Columns */
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from Customers