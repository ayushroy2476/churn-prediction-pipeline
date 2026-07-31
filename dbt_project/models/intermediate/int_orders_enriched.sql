with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

items_agg as (
    select * from {{ ref('int_order_items_agg') }}
),

payments_agg as (
    select * from {{ ref('int_order_payments_agg') }}
),

reviews as (
    select
        order_id,
        avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

-- category of the first item on the order (simplification for orders with multiple categories)
first_item_category as (
    select
        oi.order_id,
        p.product_category
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_products') }} p using (product_id)
    qualify row_number() over (partition by oi.order_id order by oi.order_item_id) = 1
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_state,
    o.order_status,
    o.order_purchase_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts,
    date_diff(
        date(o.order_delivered_customer_ts),
        date(o.order_estimated_delivery_ts),
        day
    ) as delivery_delay_days,
    coalesce(ia.num_items, 0) as num_items,
    coalesce(pa.total_payment_value, 0) as order_value,
    coalesce(ia.total_freight_value, 0) as freight_value,
    r.review_score,
    fic.product_category
from orders o
left join customers c using (customer_id)
left join items_agg ia using (order_id)
left join payments_agg pa using (order_id)
left join reviews r using (order_id)
left join first_item_category fic using (order_id)
