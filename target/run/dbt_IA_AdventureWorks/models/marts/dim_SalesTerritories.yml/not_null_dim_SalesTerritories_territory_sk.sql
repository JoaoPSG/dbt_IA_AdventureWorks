select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `valid-sol-346522`.`AdventureWorks_marts`.`dim_SalesTerritories`
where territory_sk is null



      
    ) dbt_internal_test