
    
    

with dbt_test__target as (
  
  select territory_sk as unique_field
  from `valid-sol-346522`.`AdventureWorks_marts`.`dim_SalesTerritories`
  where territory_sk is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


