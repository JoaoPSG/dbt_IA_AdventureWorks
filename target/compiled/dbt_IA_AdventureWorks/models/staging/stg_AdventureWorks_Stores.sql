with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_store`
),

Stores as (
    select
        /* Natural Key */
        businessentityid as store_id
        /* Natural Key */
        ,salespersonid as salesPerson_id
        /* Columms */
        ,source.name as storeName
        ,demographics

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from Stores