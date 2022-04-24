with
    SalesTerritory as (
        select * from {{ ref('stg_AdventureWorks_SalesTerritory') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            row_number() over (order by territory_id) as territory_sk --auto incremental surrogate key
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