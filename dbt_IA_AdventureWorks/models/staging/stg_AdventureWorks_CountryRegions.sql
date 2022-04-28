with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_countryregion') }}
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