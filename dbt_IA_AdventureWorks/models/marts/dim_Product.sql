with
    Products as (
        select * from {{ ref('stg_AdventureWorks_Products') }} 
    ),


    final as (
        select
            /* Surrogate Key */
            row_number() over (order by product_id) as product_sk --auto incremental surrogate key
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
            ,"weight"
            ,makeFlag
            ,listPrice
            ,productLine
            ,sellEndDate
            ,reorderPoint
            ,standardCost
            ,sellstartdatsellStartDate
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