import json

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

class ValidateOperator:
    """
        Class ValidateOperator: performs validations under a dataset
            according to its manifest.
        
        -> PARAMS:
            - dataset_file: path to the dataset file
            - manifest_file: path to the manifest file
            - delimiter: by default columns are delimited using ',' but delimiter can be set to any character
            - mode: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
                -> PERMISSIVE: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
                -> DROPMALFORMED: drops lines which have fewer or more tokens than expected or tokens which do not match the schema
                -> FAILFAST: aborts with a RuntimeException if encounters any malformed line

                check out more on: https://github.com/databricks/spark-csv
            - format_output: output format file. It can be either CSV, PARQUET or JSON
        
        -> METHODS:
            - generate_file: creates a curated amount of files related to the file and matching schema
            - _retrieve_schema: internal method to get the manifest schema
    """

    def __init__(self, dataset_file, manifest_file, delimiter, mode, format_output):
        self.spark = SparkSession.builder.master("local[2]").\
            config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")\
                .config("parquet.enable.summary-metadata", "false")\
                    .getOrCreate()
        self.schema = self._retrieve_schema(manifest_file)
        self.dataset_file = dataset_file
        self.delimiter = delimiter
        self.mode = mode
        self.format_output = format_output.replace('.', '')
        
    def _retrieve_schema(self, manifest_file):
        rdd = self.spark.sparkContext.wholeTextFiles(manifest_file)
        text = rdd.collect()[0][1]
        dict = json.loads(str(text))
        custom_schema = StructType.fromJson(dict)

        return custom_schema

    def generate_file(self):
        print("HANDLING DATASET WITH SCHEMA")
        try:
            dataset = self.spark.read.option("header", "true")\
                .option("mode", f"{self.mode}")\
                    .option("delimiter", f"{self.delimiter}")\
                        .format("csv")\
                            .schema(self.schema)\
                                .load(self.dataset_file)
        except:
            print(sys.exc_info()[0])

        print("SCHEMA COMPARISON:")
        print(f"DATASET SCHEMA: {dataset.schema}")
        print(f"MANIFEST SCHEMA: {self.schema}")
        print("DATASET PREVIEW: ")
        dataset.show()

        print(f"WRITING FILE IN {self.format_output.upper()} FORMAT")
        try:
            dataset.write.option("header", "true").format(self.format_output).save(f"day={str(date.today())}")
        except:
            print(sys.exc_info()[0])

def HandlerValidateOperator(dataset_file=' ', manifest_file=' ', delimiter=',', mode='PERMISSIVE', format_output='csv'):
    print("============================ STARTING CONSOLIDATION PROCCESS ============================")
    print(f"DATASET PATH FILE: {dataset_file}")
    print(f"MANIFEST PATH FILE: {manifest_file}")
    print(f"DELIMITER: {delimiter}")

    _mode_extract = mode if mode in ['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'] else 'PERMISSIVE'
    print(f"MODE: {_mode_extract}")

    _output_format = format_output if format_output.upper() in ['CSV', 'PARQUET', 'JSON'] else 'csv'
    print(f"OUTPUT FORMAT: {_output_format}")
    
    ValidateOperator(dataset_file, manifest_file, delimiter, _mode_extract, _output_format).generate_file()
