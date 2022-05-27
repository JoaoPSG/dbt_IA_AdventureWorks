with
    Products as (
        select * from {{ ref('stg_AdventureWorks_Products') }} 
    ),

    ProductSubcategories as (
        select * from {{ ref('stg_AdventureWorks_ProductSubcategories') }} 
    ),

    ProductCategories as (
        select * from {{ ref('stg_AdventureWorks_ProductCategories') }} 
    ),

    ProductModels as (
        select * from {{ ref('stg_AdventureWorks_ProductModels') }} 
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['product_id']) }} as product_sk --hashed surrogate key
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