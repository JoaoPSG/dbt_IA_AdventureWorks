with
    Products as (
        select * from {{ ref('stg_AdventureWorks_Products') }} 
    ),


    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['product_id']) }} as product_sk --hashed surrogate key
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
    )

select * from final