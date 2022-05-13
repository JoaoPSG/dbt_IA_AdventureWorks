

  create or replace view `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_SalesTerritories`
  OPTIONS()
  as with
     __dbt__cte__stg_AdventureWorks_SalesTerritories as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_salesterritory`
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
),  __dbt__cte__stg_AdventureWorks_CountryRegions as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_countryregion`
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
),SalesTerritories as (
        select * from __dbt__cte__stg_AdventureWorks_SalesTerritories
    ),

    CountryRegions as (
        select * from __dbt__cte__stg_AdventureWorks_CountryRegions
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(territory_id as 
    string
), '') || '-' || coalesce(cast(CountryRegions.countryRegion_id as 
    string
), '') as 
    string
))) as territory_sk --hashed surrogate key
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

select * from final;

