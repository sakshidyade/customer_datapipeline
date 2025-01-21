import os
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

if __name__ == "__main__":
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\datascience\gitclone\earthquack_ingestion\football_analysis\sai-project-2024-441608-f74a6798c38d.json"

    # Set up your pipeline options
    pipeline_options = PipelineOptions()

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'sai-project-2024-441608'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'readthedata'
    google_cloud_options.staging_location = 'gs://temp_buckdet'
    google_cloud_options.temp_location = 'gs://stage_buckdet'

    input2 = r"gs://temp_buckdet/customer_profile1.csv"

    def parse_csv(line):
        # split the csv file
        fields = line.split(',')
        return {
            'customer_id': fields[0],
            'first_name': fields[1],
            'last_name': fields[2],
            'email': fields[3],
            'signup_date': fields[4]
        }

    class CleanData(beam.DoFn):
        def process(self, element):

            try:
                signup_date = datetime.strptime(element['signup_date'], '%Y-%m-%d')

                clean_record = {
                    'customer_id': element['customer_id'],
                    'first_name': element['first_name'],
                    'last_name': element['last_name'],
                    'email': element['email'].lower() if element.get('email') else None,
                    'signup_date': signup_date,
                    'year': signup_date.year,
                    'month': signup_date.month,
                    'day': signup_date.day,
                }
                yield clean_record
            except Exception:
                print(f"Error processing record")

    with beam.Pipeline(options=pipeline_options) as p:
        res = (
            p
            | "ReadInput" >> beam.io.ReadFromText(input2, skip_header_lines=1)
            | "ParseCSV" >> beam.Map(parse_csv)
            | 'print' >> beam.Map(print))
            # | "CleanData" >> beam.ParDo(CleanData())
            # | "write to bq" >> beam.io.WriteToBigQuery(table="sai-project-2024-441608.new_dataset.customer_table",
            #                                            schema='SCHEMA_AUTODETECT',
            #                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            #                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

            # | "PrintResults" >> beam.Map(print)
        # )
