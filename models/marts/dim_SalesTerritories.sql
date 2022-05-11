with
    SalesTerritories as (
        select * from {{ ref('stg_AdventureWorks_SalesTerritories') }}
    ),

    CountryRegions as (
        select * from {{ ref('stg_AdventureWorks_CountryRegions') }}
    ),

    final as (
        select
            /* Surrogate Key */
            distinct {{ dbt_utils.surrogate_key(['territory_id', 'CountryRegions.countryRegion_id', 'rowguid', 'countryName', 'costYtd', 'salesYtd', 'costLastYear', 'salesLastYear']) }} as territory_sk --hashed surrogate key
            /* Natural Key */
            ,territory_id
            /* Foreing Key */
            ,CountryRegions.countryRegion_id
            /* Columms */
            ,territoryName
            ,territoryGroup
            ,costYtd
            ,salesYtd
            ,costLastYear
            ,salesLastYear
            ,rowguid
            ,countryName
        from SalesTerritories

        left join CountryRegions on SalesTerritories.countryRegion_id=CountryRegions.countryRegion_id
    )

select * from final