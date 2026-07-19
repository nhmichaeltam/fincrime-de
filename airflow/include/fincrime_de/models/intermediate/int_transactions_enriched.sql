with transactions as (
    select * from {{ ref('stg_transactions') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

cards as (
    select * from {{ ref('stg_cards') }}
),

enriched as (
    select
        -- transaction core
        t.user_id,
        t.card_index,
        t.transaction_date,
        t.transaction_time,
        t.amount,
        t.use_chip,
        t.merchant_id,
        t.merchant_city,
        t.merchant_state,
        t.merchant_zip,
        t.mcc,
        t.is_fraud,
        t.error_type,

        -- user attributes
        u.full_name,
        u.current_age,
        u.gender,
        u.city                  as customer_city,
        u.state                 as customer_state,
        u.yearly_income,
        u.total_debt,
        u.fico_score,
        u.num_credit_cards,

        -- card attributes
        c.card_brand,
        c.card_type,
        c.has_chip,
        c.credit_limit,
        c.card_on_dark_web

    from transactions t
    left join users u on t.user_id = u.user_id
    left join cards c on t.user_id = c.user_id and t.card_index = c.card_index
)

select * from enriched
