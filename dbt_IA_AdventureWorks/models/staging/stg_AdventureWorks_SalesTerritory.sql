with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_creditcard') }}
),

SalesTerritory as (
    select
        /* Natural Key */
        territoryid as territory_id
        /* Columms */
        ,"name" as territoryName
        ,group
        ,costytd as costYtd
        ,salesytd as salesYtd
        ,costlastyear as costLastYear
        ,saleslastyear as salesLastYear
        ,countryregioncode as countryRegionCode
        ,rowguid
        ,cast(modifieddate as date) as modifiedDate
    from source
)

select * from SalesTerritory