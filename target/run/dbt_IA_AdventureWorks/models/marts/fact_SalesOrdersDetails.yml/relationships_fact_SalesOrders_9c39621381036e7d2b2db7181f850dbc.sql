select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select creditCard_sk as from_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`
    where creditCard_sk is not null
),

parent as (
    select id as to_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_CreditCards`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test