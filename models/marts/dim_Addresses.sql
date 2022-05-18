with
    Addresses as (
        select * from {{ ref('stg_AdventureWorks_Addresses') }} 
    ),

    StateProvinces as (
        select * from {{ ref('stg_AdventureWorks_StateProvinces') }} 
    ),

    CountryRegions as (
        select * from {{ ref('stg_AdventureWorks_CountryRegions') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['address_id']) }} as address_sk --hashed surrogate key
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