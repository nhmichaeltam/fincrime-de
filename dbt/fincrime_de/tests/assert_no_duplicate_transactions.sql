{{ config(severity='warn') }}

-- Defensive tripwire against duplicate transactions.
--
-- The incremental ingestion load is idempotent (delete-then-append per source
-- day), so no transaction should ever appear more than once. This test flags any
-- that do — a signal that a day may have been loaded twice. It is warn-level, not
-- error-level, because two genuinely distinct transactions could in principle
-- share the same natural key (the source time is only precise to the minute), so
-- this surfaces duplication for investigation rather than failing the pipeline.

select
    user_id,
    card_index,
    transaction_date,
    transaction_time,
    amount,
    merchant_id,
    count(*) as n_copies
from {{ ref('stg_transactions') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
