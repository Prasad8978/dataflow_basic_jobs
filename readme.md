--------------------
the pipeline design:
--------------------
++++++++++++++++++++++++++++++
   ETL-BATCH DATA PIPELINES:
++++++++++++++++++++++++++++++


    source -------> landing zone(gcp) --------> edw(bigquery) [stage, final, view] --> looker(bi)
    
    oracle
    mysql  --csv--> gcs landing zone(bucket) --->dataflow[archestrated by airflow)--> edw(bq)---
    ------->[stage, final, view] ----> bi [looker ]
    
    
Oracle / MySQL
     ⬇️ (CSV Export)
GCS (Landing Zone Bucket)
     ⬇️ (Airflow triggers Dataflow jobs)
Dataflow (ETL jobs)
     ⬇️
BigQuery (Stage → Final → View)
     ⬇️
Looker (Dashboards & Analytics)


# 2.gcs_to_bq_min_max_workers_runner_cli:

python D:\Cloud-DE\GCP\dataflow\2.gcs_to_bq_min_max_workers_runner_cli.py `
  --input gs://bucket-for-beam1/emp8_data.csv `
  --output rare-tome-458105-n0:dataflow.emp_stage_table_1 `
  --project rare-tome-458105-n0 `
  --region us-central1 `
  --staging_location gs://bucket-for-beam1/staging `
  --temp_location gs://bucket-for-beam1/temp `
  --job_name employee-dataflow-job-1 `
  --runner DataflowRunner `
  --save_main_session True `
  --autoscaling_algorithm THROUGHPUT_BASED `
  --num_workers 2 `
  --max_num_workers 10 `
  --worker_machine_type n1-standard-1


# 3.gcs_to_bq_end_to_end:

python D:\Cloud-DE\GCP\dataflow\3.gcs_to_bq_end_to_end.py `
  --input gs://bucket-for-beam1/emp8_data.csv `
  --output rare-tome-458105-n0:dataflow.emp_stage_table_1 `
  --project rare-tome-458105-n0 `
  --region us-central1 `
  --staging_location gs://bucket-for-beam1/staging `
  --temp_location gs://bucket-for-beam1/temp `
  --job_name employee-dataflow-job-2 `
  --autoscaling_algorithm THROUGHPUT_BASED