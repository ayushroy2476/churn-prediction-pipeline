with items as (
    select * from {{ ref('stg_order_items') }}
)

select
    order_id,
    count(*) as num_items,
    sum(price) as total_item_price,
    sum(freight_value) as total_freight_value,
    count(distinct product_id) as num_distinct_products
from items
group by order_id
