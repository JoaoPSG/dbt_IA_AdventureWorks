with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_countryregion`
),

CountryRegions as (
    select
        /* Natural Key */
        countryregioncode as countryRegion_id
        /* Columms */
        ,"name" as countryName
        ,modifieddate as modifiedDate
    from source
)

select * from CountryRegions