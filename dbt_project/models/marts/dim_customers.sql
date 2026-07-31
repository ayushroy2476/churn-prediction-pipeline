select
    customer_unique_id,
    any_value(customer_state) as customer_state,
    min(order_purchase_ts) as first_order_ts,
    max(order_purchase_ts) as last_order_ts,
    count(distinct order_id) as lifetime_orders,
    sum(order_value) as lifetime_value
from {{ ref('int_orders_enriched') }}
group by customer_unique_id
