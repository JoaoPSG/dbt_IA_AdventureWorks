with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_productcategory') }}
),

ProductCategories as (
    select
        /* Natural Key */
        productcategoryid as productCategory_id
        /* Columms */
        ,source.name as productCategory

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from ProductCategories