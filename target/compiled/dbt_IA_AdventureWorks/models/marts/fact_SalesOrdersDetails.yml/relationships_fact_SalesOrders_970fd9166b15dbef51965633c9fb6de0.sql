
    
    

with child as (
    select creditCard_sk as from_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`
    where creditCard_sk is not null
),

parent as (
    select creditCard_sk as to_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_CreditCards`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


