# Getting the dataset

This project uses the **Olist Brazilian E-Commerce Public Dataset** from Kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Option 1: Kaggle API (recommended)
1. Create a Kaggle account and generate an API token: Account → Create New Token.
   This downloads a `kaggle.json` file.
2. Place it at `~/.kaggle/kaggle.json` and run `chmod 600 ~/.kaggle/kaggle.json`.
3. Install the CLI: `pip install kaggle`
4. Download and unzip straight into this folder:
   ```bash
   kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
   ```

## Option 2: Manual download
Download the ZIP from the Kaggle page above, extract it, and place all CSVs in `data/raw/`.

## Expected files in `data/raw/`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

If Kaggle ever changes a column name, `scripts/ingest_to_bigquery.py` loads with
`autodetect=True`, but the dbt staging models expect the column names listed
above (as of the dataset's current version) — worth a quick sanity check with
`df.columns` after download.
