with
    Products as (
        select * from {{ ref('stg_AdventureWorks_Products') }} 
    ),


    surrogate as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['product_id', 'productModel_id', 'productSubcategory_id', 'rowguid']) }} as product_sk --hashed surrogate key
            /* Natural Key */
            ,product_id
            /* Foreing Key */
            ,productModel_id
            ,productSubcategory_id
            /* Columns */
            ,productName
            ,productNumber
        
            ,size
            ,class
            ,color
            ,style
            ,productWeight
            ,makeFlag
            ,listPrice
            ,productLine
            ,sellEndDate
            ,reorderPoint
            ,standardCost
            ,sellStartDate
            ,discontinuedDate
            ,safetyStockLevel
            ,daysToManufacture
            ,finishedGoodsFlag
            ,sizeUnitMmeasureCode
            ,weightUnitMeasureCode
            ,rowguid
        from Products
    ),

    final as (
        select
            /* Surrogate Key */
            distinct product_sk
            /* Natural Key */
            ,product_id
            /* Foreing Key */
            ,productModel_id
            ,productSubcategory_id
            /* Columns */
            ,productName
            ,productNumber

            ,size
            ,class
            ,color
            ,style
            ,productWeight
            ,makeFlag
            ,listPrice
            ,productLine
            ,sellEndDate
            ,reorderPoint
            ,standardCost
            ,sellStartDate
            ,discontinuedDate
            ,safetyStockLevel
            ,daysToManufacture
            ,finishedGoodsFlag
            ,sizeUnitMmeasureCode
            ,weightUnitMeasureCode

        from surrogate
    )

select * from final