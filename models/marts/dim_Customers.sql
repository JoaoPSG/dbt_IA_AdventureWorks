with
    Customers as (
        select * from {{ ref('stg_AdventureWorks_Customers') }} 
    ),

    Persons as (
        select * from {{ ref('stg_AdventureWorks_Persons') }}
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['customer_id']) }} as customer_sk --hashed surrogate key
            /* Natural Key */
            ,customer_id
            /* Foreing Key */
            ,Customers.person_id
            ,territory_id
            /* Columms */ 
            ,personType
            ,title
            ,suffix
            ,firstName
            ,middleName
            ,lastName
            ,nameStyle
            ,emailPromotion
            ,additionalContactInfo
        from Customers

        left join Persons on Customers.person_id = Persons.persons_id
    )

select * from final