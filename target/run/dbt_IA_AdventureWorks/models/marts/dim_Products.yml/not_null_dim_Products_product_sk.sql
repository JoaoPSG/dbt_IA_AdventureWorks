select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Products`
where product_sk is null



      
    ) dbt_internal_test