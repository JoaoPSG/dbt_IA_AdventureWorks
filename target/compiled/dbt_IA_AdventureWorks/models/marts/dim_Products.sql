with
     __dbt__cte__stg_AdventureWorks_Products as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_product`
),

Products as (
    select
        /* Natural Key */
        productid as product_id
        /* Foreing Key */
        ,productmodelid as productModel_id
        ,productsubcategoryid as productSubcategory_id
        /* Columns */
        ,source.name as productName
        ,productnumber as productNumber
    
        ,size
        ,class
        ,color
        ,style
        ,source.weight as productWeight
        ,makeflag as makeFlag
        ,listprice as listPrice
        ,productline as productLine
        ,cast(sellenddate as timestamp) as sellEndDate
        ,reorderpoint as reorderPoint
        ,standardcost as standardCost
        ,cast(sellstartdate as timestamp) as sellStartDate
        ,cast(discontinueddate as timestamp) as discontinuedDate
        ,safetystocklevel as safetyStockLevel
        ,daystomanufacture as daysToManufacture
        ,finishedgoodsflag as finishedGoodsFlag
        ,sizeunitmeasurecode as sizeUnitMmeasureCode
        ,weightunitmeasurecode as weightUnitMeasureCode
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from Products
),  __dbt__cte__stg_AdventureWorks_ProductSubcategories as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_productsubcategory`
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
),  __dbt__cte__stg_AdventureWorks_ProductCategories as (
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
),  __dbt__cte__stg_AdventureWorks_ProductModels as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_productmodel`
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
),Products as (
        select * from __dbt__cte__stg_AdventureWorks_Products 
    ),

    ProductSubcategories as (
        select * from __dbt__cte__stg_AdventureWorks_ProductSubcategories 
    ),

    ProductCategories as (
        select * from __dbt__cte__stg_AdventureWorks_ProductCategories 
    ),

    ProductModels as (
        select * from __dbt__cte__stg_AdventureWorks_ProductModels 
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(product_id as 
    string
), '') as 
    string
))) as product_sk --hashed surrogate key
            /* Natural Key */
            ,product_id
            /* Foreing Key */
            -- ,productModel_id
            -- ,ProductSubcategories.productSubcategory_id
            /* Columns */
            ,productName
            ,productNumber
            ,ProductModels.productModel
            ,ProductSubcategories.productSubcategory
            ,ProductCategories.productCategory
        
            -- ,size
            -- ,class
            -- ,color
            -- ,style
            -- ,productWeight
            -- ,makeFlag
            -- ,listPrice
            -- ,productLine
            -- ,sellEndDate
            -- ,reorderPoint
            -- ,standardCost
            -- ,sellStartDate
            -- ,discontinuedDate
            -- ,safetyStockLevel
            -- ,daysToManufacture
            -- ,finishedGoodsFlag
            -- ,sizeUnitMmeasureCode
            -- ,weightUnitMeasureCodes
        from Products

        left join ProductModels on Products.productModel_id = ProductModels.productModel_id
        left join ProductSubcategories on Products.productSubcategory_id = ProductSubcategories.productSubcategory_id
        left join ProductCategories on ProductSubcategories.productCategory_id = ProductCategories.productCategory_id        
    )

select * from final