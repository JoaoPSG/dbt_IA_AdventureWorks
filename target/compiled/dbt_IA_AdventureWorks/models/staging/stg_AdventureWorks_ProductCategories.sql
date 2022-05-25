with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_productcategory`
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