with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_productsubcategory`
),

ProductSubcategory as (
    select
        /* Natural Key */
        productsubcategoryid as productSubcategory_id
        /* Foreing Key */
        ,productcategoryid as productCategory_id
        /* Columms */
        ,source.name as productSubcategory

        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from ProductSubcategory