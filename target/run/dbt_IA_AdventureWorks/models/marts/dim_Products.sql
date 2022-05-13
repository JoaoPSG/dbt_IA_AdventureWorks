

  create or replace table `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Products`
  
  
  OPTIONS()
  as (
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
        ,"name" as productName
        ,productnumber as productNumber
    
        ,size
        ,class
        ,color
        ,style
        ,"weight" as productWeight
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
),Products as (
        select * from __dbt__cte__stg_AdventureWorks_Products 
    ),


    surrogate as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(product_id as 
    string
), '') || '-' || coalesce(cast(productModel_id as 
    string
), '') || '-' || coalesce(cast(productSubcategory_id as 
    string
), '') as 
    string
))) as product_sk --hashed surrogate key
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
            product_sk
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
  );
  