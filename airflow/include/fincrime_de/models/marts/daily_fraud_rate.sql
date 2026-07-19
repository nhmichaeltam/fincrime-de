select
    date(transaction_date)                                              as transaction_day,
    count(*)                                                            as total_transactions,
    countif(is_fraud)                                                   as fraud_transactions,
    sum(amount)                                                         as total_amount,
    sum(case when is_fraud then amount else 0 end)                      as fraud_amount,
    round(safe_divide(countif(is_fraud), count(*)) * 100, 4)           as fraud_rate_pct,
    round(safe_divide(
        sum(case when is_fraud then amount else 0 end), sum(amount)
    ) * 100, 4)                                                         as fraud_amount_rate_pct

from {{ ref('int_transactions_enriched') }}
group by 1
