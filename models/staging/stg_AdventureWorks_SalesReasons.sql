with source as (
    select * from {{ source( 'AdventureWorks_BQ', 'airbyte_salesreason') }}
),

SalesReason as (
    select
        /* Natural Key */
        salesreasonid as salesReason_id
        /* Columns */
        ,source.name as reasonName
        ,reasontype as reassonType
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from SalesReason