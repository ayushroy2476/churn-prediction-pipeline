with payments as (
    select * from {{ ref('stg_order_payments') }}
)

select
    order_id,
    sum(payment_value) as total_payment_value,
    max(payment_installments) as max_installments,
    -- primary payment method: the one used in the first payment sequence for the order
    array_agg(payment_type order by payment_sequential limit 1)[offset(0)] as primary_payment_type
from payments
group by order_id
