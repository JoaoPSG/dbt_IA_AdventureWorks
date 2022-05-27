with source as (
    select * from `snappy-meridian-350123`.`AdventureWorks_raw`.`airbyte_person`
),

Persons as (
    select
        /* Natural Key */
        businessentityid as person_id
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