

  create or replace table `valid-sol-346522`.`AdventureWorks_marts`.`dim_Product`
  
  
  OPTIONS()
  as (
    with
     __dbt__cte__stg_AdventureWorks_Products as (
with source as (
    select * from `valid-sol-346522`.`AdventureWorks`.`airbyte_product`
),

Products as (
    select
        /* Natural Key */
        productid as product_id
        /* Foreing Key */
        ,productmodelid as productModel_id
        ,productsubcategoryid as productSubcategory_id
        /* Columns */
        "name" as productName
        ,productnumber as productNumber
    
        ,size
        ,class
        ,color
        ,style
        ,"weight"
        ,makeflag as makeFlag
        ,listprice as listPrice
        ,productline as productLine
        ,cast(sellenddate as date) as sellEndDate
        ,reorderpoint as reorderPoint
        ,standardcost as standardCost
        ,cast(sellstartdate as date) as sellStartDate
        ,cast(discontinueddate as date) as discontinuedDate
        ,safetystocklevel as safetyStockLevel
        ,daystomanufacture as daysToManufacture
        ,finishedgoodsflag as finishedGoodsFlag
        ,sizeunitmeasurecode as sizeUnitMmeasureCode
        ,weightunitmeasurecode as weightUnitMeasureCode
        ,rowguid
        ,cast(modifieddate as date) as modifiedDate
    from source
)

select * from Products
),Products as (
        select * from __dbt__cte__stg_AdventureWorks_Products 
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
  );
  