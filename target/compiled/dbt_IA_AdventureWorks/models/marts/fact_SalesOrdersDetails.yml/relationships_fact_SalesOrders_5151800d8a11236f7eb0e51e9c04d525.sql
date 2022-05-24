
    
    

with child as (
    select reason_sk as from_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`fact_SalesOrdersDetails`
    where reason_sk is not null
),

parent as (
    select reason_sk as to_field
    from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


