

  create or replace view `snappy-meridian-350123`.`AdventureWorks_marts`.`dim_Customers`
  OPTIONS()
  as with
     __dbt__cte__stg_AdventureWorks_Customers as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_customer`
),

Customers as (
    select
        /* Natural Key */
        customerid as customer_id
        /* Foreing Key */
        ,personid as person_id
        ,territoryid as territory_id
        -- ,storeid as store_id
        /* Columns */
        ,rowguid
        ,cast(modifieddate as timestamp) as modifiedDate
    from source
)

select * from Customers
),  __dbt__cte__stg_AdventureWorks_Persons as (
with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks`.`airbyte_person`
),

Persons as (
    select
        /* Natural Key */
        businessentityid as persons_id
        /* Columms */        
        ,persontype as personType
        ,title
        ,suffix
        ,firstname as firstName
        ,middlename as middleName
        ,lastname as lastName
        ,namestyle as nameStyle
        --,demographics --not useful
        ,emailpromotion as emailPromotion
        ,additionalcontactinfo as additionalContactInfo
        
        ,rowguid
        ,modifieddate as modifiedDate
    from source
)

select * from Persons
),Customers as (
        select * from __dbt__cte__stg_AdventureWorks_Customers 
    ),

    Persons as (
        select * from __dbt__cte__stg_AdventureWorks_Persons
    ),

    final as (
        select
            /* Surrogate Key */
            to_hex(md5(cast(coalesce(cast(customer_id as 
    string
), '') as 
    string
))) as customer_sk --hashed surrogate key
            /* Natural Key */
            ,customer_id
            /* Foreing Key */
            ,Customers.person_id
            ,territory_id
            /* Columms */ 
            ,personType
            -- ,title
            -- ,suffix
            ,firstName
            ,middleName
            ,lastName
            -- ,nameStyle
            -- ,emailPromotion
            -- ,additionalContactInfo
        from Customers

        left join Persons on Customers.person_id = Persons.persons_id
    )

select * from final;

