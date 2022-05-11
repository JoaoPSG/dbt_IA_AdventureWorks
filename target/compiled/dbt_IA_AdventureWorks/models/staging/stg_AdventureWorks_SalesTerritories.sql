with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_salesterritory`
),

SalesTerritories as (
    select
        /* Natural Key */
        territoryid as territory_id
        /* Foreing Key */
        ,countryregioncode as countryRegion_id
        /* Columms */
        ,"name" as territoryName
        ,"group" as territoryGroup
        ,costytd as costYtd
        ,salesytd as salesYtd
        ,costlastyear as costLastYear
        ,saleslastyear as salesLastYear
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesTerritories