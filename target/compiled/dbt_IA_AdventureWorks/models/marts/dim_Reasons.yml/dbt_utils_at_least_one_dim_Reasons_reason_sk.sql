

select *
from (
    select
        
        
      count(reason_sk) as filler_column

    from `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Reasons`

    having count(reason_sk) = 0

) validation_errors

