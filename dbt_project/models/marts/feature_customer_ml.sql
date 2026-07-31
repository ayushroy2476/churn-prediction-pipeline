-- Features are computed only from each customer's FIRST order.
-- This is deliberate: at prediction time (right after a customer's first
-- purchase), nothing about their second order is knowable yet, so building
-- features from later orders would leak the label.

with ranked_orders as (
    select
        *,
        row_number() over (
            partition by customer_unique_id order by order_purchase_ts
        ) as order_rank
    from {{ ref('int_orders_enriched') }}
),

first_orders as (
    select *
    from ranked_orders
    where order_rank = 1
),

second_orders as (
    select
        customer_unique_id,
        order_purchase_ts as second_order_ts
    from ranked_orders
    where order_rank = 2
)

select
    fo.customer_unique_id,
    fo.order_id as first_order_id,
    fo.order_purchase_ts as first_order_ts,
    fo.customer_state,
    fo.order_value as first_order_value,
    fo.num_items as first_order_num_items,
    fo.freight_value as first_order_freight_value,
    fo.delivery_delay_days as first_order_delivery_delay_days,
    fo.review_score as first_order_review_score,
    fo.product_category as first_order_product_category,
    so.second_order_ts,
    case
        when so.second_order_ts is not null
            and date_diff(date(so.second_order_ts), date(fo.order_purchase_ts), day) <= 90
        then 1
        else 0
    end as repeat_within_90d
from first_orders fo
left join second_orders so using (customer_unique_id)
