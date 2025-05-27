import apache_beam as beam
import csv
from io import StringIO
from apache_beam.options.pipeline_options import PipelineOptions


file_path = "gs://bucket-for-beam1/emp8_data.csv"

class ParseAndTransformRecord(beam.DoFn):
    def process(self, element):
        # Use csv reader to handle quoted addresses properly
        reader = csv.reader(StringIO(element))
        row = next(reader)

        if len(row) != 8:
            return # skip bad rows
        emp_id, name, age, dept, designation, salary, phone, address = row

        # split rows
        try:
            street, city, state = [x.strip() for x in address.split(',')]
        except ValueError:
            street, city, state = address, "",""

        yield {
            'id': int(emp_id),
            'name': name,
            'age': int(age),
            'department': dept,
            'designation': designation,
            'salary': float(salary),
            'phone': phone,
            'street': street,
            'city': city,
            'state': state
        }

def get_pipeline_options():
    options = PipelineOptions([
        "--runner=DataflowRunner",
        "--project=rare-tome-458105-n0",
        "--region=us-central1",
        "--job_name=employee-dataflow-job",
        "--staging_location=gs://bucket-for-beam1/staging",
        "--temp_location=gs://bucket-for-beam1/temp",
        "--save_main_session",
        "--num_workers=2",
        "--worker_machine_type=n1-standard-1"
    ])  

    return options

pipeline_options = get_pipeline_options()

bq_table = 'rare-tome-458105-n0.dataflow.emp_stage_table' # output => bq stage table

# BigQuery schema
'''bq_schema = {
    "fields" : [
        {"name" : "id", "type" : "INTEGER"},
        {"name" : "name", "type" : "STRING"},
        {"name" : "age", "type" : "INTEGER"},
        {"name" : "department", "type" : "STRING"},
        {"name" : "designation", "type" : "STRING"},
        {"name" : "salary", "type" : "FLOAT"},
        {"name" : "phone", "type" : "STRING"},
        {"name" : "street", "type" : "STRING"},
        {"name" : "city", "type" : "STRING"},
        {"name" : "state", "type" : "STRING"}
    ]
}
'''
bq_schema = "id:INTEGER,name:STRING,age:INTEGER,department:STRING,designation:STRING,salary:FLOAT,phone:STRING,street:STRING,city:STRING,state:STRING"

with beam.Pipeline(options = pipeline_options) as p:
    data = ( 
        p | 'reading data' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
          | 'pardo calling' >> beam.ParDo(ParseAndTransformRecord())
          | 'write into bq' >> beam.io.WriteToBigQuery(
              table = bq_table,
              schema = bq_schema,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
          )
    )