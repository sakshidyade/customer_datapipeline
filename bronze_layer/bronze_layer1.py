
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

def run_pipeline():
    pipeline_options = PipelineOptions()

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'sai-project-2024-441608'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'readthedata'
    google_cloud_options.staging_location = 'gs://temp_buckdet/'
    google_cloud_options.temp_location = 'gs://stage_buckdet/'

    input2 = 'gs://temp_buckdet/customer_transcation1.csv'

    def parse_csv(line):
        fields = line.split(',')
        return {
            'transaction_id': fields[0],
            'customer_id': fields[1],
            'transaction_date': fields[2],
            'amount': float(fields[3]),
            'category': fields[4]
        }

    class LatestRecord(beam.DoFn):
        def process(self, element):
            key, records = element
            latest_record = max(
                records,
                key=lambda x: x['transaction_date']
            )
            yield latest_record

    class CleanData(beam.DoFn):
        def process(self, element):
            try:
                transaction_amount = float(element['amount'])
                is_large_transaction = transaction_amount > 1000

                transaction_date = element['transaction_date']
                transaction_datetime = datetime.strptime(transaction_date, '%Y-%m-%d %H:%M:%S')
                year = transaction_datetime.year
                month = transaction_datetime.month
                day = transaction_datetime.day

                standardized_category = element['category'].lower()

                clean_record = {
                    'transaction_id': element['transaction_id'],
                    'customer_id': element['customer_id'],
                    'transaction_date': element['transaction_date'],
                    'amount': element['amount'],
                    'category': standardized_category,
                    'is_large_transaction': is_large_transaction,
                    'year': year,
                    'month': month,
                    'day': day,
                }
                yield clean_record
            except Exception as e:
                print(f"Error while processing record {element['transaction_id']}: {e}")

    with beam.Pipeline(options=pipeline_options) as p:
        _ = (
            p
            | "ReadInput" >> beam.io.ReadFromText(input2, skip_header_lines=1)
            | "ParseCSV" >> beam.Map(parse_csv)
            | "PairCustomerId" >> beam.Map(lambda x: (x['customer_id'], x))
            | "GroupByCustomerId" >> beam.GroupByKey()
            | "GetLatestRecord" >> beam.ParDo(LatestRecord())
            | "CleanData" >> beam.ParDo(CleanData())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table="sai-project-2024-441608.my_dataset.transation_table1",
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run_pipeline()

