with
    Customers as (
        select * from {{ ref('stg_AdventureWorks_Customers') }} 
    ),

    Persons as (
        select * from {{ ref('stg_AdventureWorks_Persons') }}
    ),

    Stores as (
        select * from {{ ref('stg_AdventureWorks_Stores') }}
    ),

    final as (
        select
            /* Surrogate Key */
            {{ dbt_utils.surrogate_key(['customer_id']) }} as customer_sk --hashed surrogate key
            /* Natural Key */
            ,customer_id
            /* Columms */ 
            ,personType
            -- ,title
            -- ,suffix
            ,firstName
            ,middleName
            ,lastName
            ,Stores.storeName
            -- ,nameStyle
            -- ,emailPromotion
            -- ,additionalContactInfo
        from Customers

        left join Persons on Customers.person_id = Persons.person_id
        left join Stores on Customers.store_id = Stores.store_id
    )

select * from final