with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_countryregion`
),

CountryRegions as (
    select
        /* Natural Key */
        countryregioncode as countryRegion_id
        /* Columms */
        ,source.name as countryName
        ,modifieddate as modifiedDate
    from source
)

select * from CountryRegions