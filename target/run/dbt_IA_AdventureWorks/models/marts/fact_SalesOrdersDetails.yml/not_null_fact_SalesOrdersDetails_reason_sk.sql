select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`
where reason_sk is null



      
    ) dbt_internal_test