with source as (
    select * from {{ source('raw', 'cards') }}
),

renamed as (
    select
        user                                                            as user_id,
        card_index,
        card_brand,
        card_type,
        card_number,
        expires,
        has_chip,
        cards_issued,
        cast(replace(replace(credit_limit, '$', ''), ',', '') as numeric) as credit_limit,
        acct_open_date,
        year_pin_last_changed,
        case when card_on_dark_web = 'Yes' then true else false end     as card_on_dark_web

    from source
)

select * from renamed
