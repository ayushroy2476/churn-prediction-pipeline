with source as (
    select * from {{ source('olist_raw', 'products') }}
),

translated as (
    select * from {{ ref('stg_product_category_translation') }}
)

select
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name) as product_category,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from source p
left join translated t
    on p.product_category_name = t.product_category_name
