with
     __dbt__cte__stg_AdventureWorks_Addresses as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_address`
),

Addresses as (
    select
        /* Natural Key */
        addressid as address_id
        /* Foreing Key */
        ,stateprovinceid as stateProvince_id 
        /* Columms */
        ,postalcode as postalCode
        ,addressline1 as addressLine1
        ,addressline2 as addressLine2
        ,city
        ,spatiallocation as spatialLocation

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from Addresses
),  __dbt__cte__stg_AdventureWorks_StateProvinces as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_stateprovince`
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
),  __dbt__cte__stg_AdventureWorks_CountryRegions as (
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
),Addresses as (
        select * from __dbt__cte__stg_AdventureWorks_Addresses 
    ),

    StateProvinces as (
        select * from __dbt__cte__stg_AdventureWorks_StateProvinces 
    ),

    CountryRegions as (
        select * from __dbt__cte__stg_AdventureWorks_CountryRegions 
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(address_id as 
    string
), '') as 
    string
))) as address_sk --hashed surrogate key
            /* Natural Key */
            ,address_id
            /* Columms */ 
            ,postalCode
            ,addressLine1
            ,addressLine2
            ,city
            ,spatialLocation

            ,provinceName
            ,stateProvinceCode
            ,isOnlyStateProvinceFlag

            ,countryName
        from Addresses

        left join StateProvinces on Addresses.stateProvince_id = StateProvinces.stateProvince_id
        left join CountryRegions on StateProvinces.countryRegion_id = CountryRegions.countryRegion_id
    )

select * from final