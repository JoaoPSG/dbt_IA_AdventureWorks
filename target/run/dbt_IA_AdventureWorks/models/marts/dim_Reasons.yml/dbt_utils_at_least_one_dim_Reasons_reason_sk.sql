select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

select *
from (
    select
        
        
      count(reason_sk) as filler_column

    from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`

    having count(reason_sk) = 0

) validation_errors


      
    ) dbt_internal_test