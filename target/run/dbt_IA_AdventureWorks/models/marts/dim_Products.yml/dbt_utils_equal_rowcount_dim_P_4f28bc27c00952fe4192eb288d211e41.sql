select
      coalesce(diff_count, 0) as failures,
      coalesce(diff_count, 0) != 0 as should_warn,
      coalesce(diff_count, 0) != 0 as should_error
    from (
      

with  __dbt__cte__stg_AdventureWorks_Products as (
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
),a as (

    select count(*) as count_a from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Products`

),
b as (

    select count(*) as count_b from __dbt__cte__stg_AdventureWorks_Products

),
final as (

    select
        count_a,
        count_b,
        abs(count_a - count_b) as diff_count
    from a
    cross join b

)

select * from final


      
    ) dbt_internal_test