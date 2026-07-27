with transaction_stats as (
    select
        user_id,
        full_name,
        gender,
        current_age,
        customer_state,
        yearly_income,
        total_debt,
        fico_score,
        num_credit_cards,
        count(*)                                                                as total_transactions,
        countif(is_fraud)                                                       as fraud_count,
        round(safe_divide(countif(is_fraud), count(*)) * 100, 4)               as fraud_rate_pct,
        sum(amount)                                                             as total_spend,
        avg(amount)                                                             as avg_transaction_amount,
        max(amount)                                                             as max_transaction_amount,
        countif(error_type is not null)                                         as error_count,
        round(safe_divide(countif(error_type is not null), count(*)) * 100, 4) as error_rate_pct
    from {{ ref('int_transactions_enriched') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

scored as (
    select
        *,
        round(
            (fraud_rate_pct * 0.5)
            + (error_rate_pct * 0.2)
            + case
                when fico_score < 580 then 30
                when fico_score < 670 then 20
                when fico_score < 740 then 10
                else 0
            end,
            2
        ) as risk_score
    from transaction_stats
)

select * from scored
