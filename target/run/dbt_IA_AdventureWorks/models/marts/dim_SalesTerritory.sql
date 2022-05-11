

  create or replace table `valid-sol-346522`.`AdventureWorks_marts`.`dim_SalesTerritory`
  
  
  OPTIONS()
  as (
    with
     __dbt__cte__stg_AdventureWorks_SalesTerritory as (
with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_salesterritory`
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
),SalesTerritory as (
        select * from __dbt__cte__stg_AdventureWorks_SalesTerritory 
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(territory_id as 
    string
), '') as 
    string
))) as territory_sk --hashed surrogate key
            /* Natural Key */
            ,territory_id
            /* Columms */
            ,territoryName
            ,group
            ,costYtd
            ,salesYtd
            ,costLastYear
            ,salesLastYear
            ,countryRegionCode
            ,rowguid
        from SalesTerritory
    )

select * from final
  );
  