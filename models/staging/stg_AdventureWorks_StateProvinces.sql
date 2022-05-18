with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_stateprovince') }}
),

StateProvinces as (
    select
        /* Natural Key */
        stateprovinceid as stateProvince_id
        /* Foreing Key */
        ,territoryid as territory_id 
        ,countryregioncode as countryRegion_id
        /* Columms */
        ,source.name as provinceName
        ,stateprovincecode as stateProvinceCode
        ,isonlystateprovinceflag as isOnlyStateProvinceFlag

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from StateProvinces