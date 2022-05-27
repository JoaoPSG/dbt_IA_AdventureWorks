with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_productmodel') }}
),

ProductModels as (
    select
        /* Natural Key */
        productmodelid as productModel_id
        /* Columms */
        ,source.name as productModel
        ,instructions
        ,catalogdescription as catalogDescription

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from ProductModels