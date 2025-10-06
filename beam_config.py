import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                    dest='input', 
                    required=True, 
                    help='Input file path in GCS')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input

options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options=options)

cleaned_data = (
    p)