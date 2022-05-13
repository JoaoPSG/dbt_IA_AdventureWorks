
    
    

with dbt_test__target as (
  
  select creditCard_sk as unique_field
  from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_CreditCards`
  where creditCard_sk is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


