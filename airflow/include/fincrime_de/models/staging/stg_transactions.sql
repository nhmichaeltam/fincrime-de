with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        user                                                            as user_id,
        card                                                            as card_index,

        transaction_date,
        time                                                            as transaction_time,

        cast(replace(replace(amount, '$', ''), ',', '') as numeric)    as amount,

        merchant_name                                                   as merchant_id,
        merchant_city,
        merchant_state,
        cast(cast(zip as int64) as string)                             as merchant_zip,
        mcc,
        use_chip,

        case when is_fraud = 'Yes' then true else false end             as is_fraud,
        nullif(trim(errors), '')                                        as error_type

    from source
)

select * from renamed
