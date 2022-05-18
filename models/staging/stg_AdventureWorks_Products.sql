with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_product') }}
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