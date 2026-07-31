.PHONY: ingest dbt-build dbt-test train score dashboard pipeline

ingest:
	python scripts/ingest_to_bigquery.py --data-dir data/raw

dbt-build:
	cd dbt_project && dbt build

dbt-test:
	cd dbt_project && dbt test

train:
	python scripts/train_model.py

score:
	python scripts/score_and_upload.py

dashboard:
	streamlit run dashboard/streamlit_app.py

pipeline: ingest dbt-build dbt-test score
