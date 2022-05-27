with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_address`
),

Addresses as (
    select
        /* Natural Key */
        addressid as address_id
        /* Foreing Key */
        ,stateprovinceid as stateProvince_id 
        /* Columms */
        ,postalcode as postalCode
        ,addressline1 as addressLine1
        ,addressline2 as addressLine2
        ,city
        ,spatiallocation as spatialLocation

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from Addresses