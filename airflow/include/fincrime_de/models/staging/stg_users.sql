with source as (
    select * from {{ source('raw', 'users') }}
),

renamed as (
    select
        user_id,
        person                                                                      as full_name,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        address,
        cast(apartment as string)                                                   as apartment,
        city,
        state,
        lpad(cast(zipcode as string), 5, '0')                                      as zipcode,
        latitude,
        longitude,
        cast(replace(replace(per_capita_income_zipcode, '$', ''), ',', '') as numeric) as per_capita_income_zipcode,
        cast(replace(replace(yearly_income_person, '$', ''), ',', '') as numeric)  as yearly_income,
        cast(replace(replace(total_debt, '$', ''), ',', '') as numeric)             as total_debt,
        fico_score,
        num_credit_cards

    from source
)

select * from renamed
