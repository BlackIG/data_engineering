# Databricks notebook source
class Datasource:
    """
    Abstract class
    Datasource class to hold the information about the datasource
    """
    def __init__(self,  path):
        """
        Constructor for the Datasource class
        """
        self.path = path

    def get_data_frame(self):
        """
        Abstract method. Function will be defined in sub classes
        """
        raise ValueError("Not implemented")

class CSVDatasource(Datasource):
    def get_data_frame(self):
        return (
            spark.
            read.format("csv").
            option("header", "true").
            load(self.path)
        )
    
class ParquetDatasource(Datasource):
    def get_data_frame(self):
        return (
            spark.
            read.
            format("parquet").
            option("header", "true").
            load(self.path)
        )

class DeltaDatasource(Datasource):
    def get_data_frame(self):
        table_name = self.path
        return (
            spark.
            read.
            table(table_name)
        )


def get_data_source(data_type, file_path):
    if data_type == "csv":
        print("CSV file reader factory")
        return CSVDatasource(file_path)
    elif data_type == "parquet":
        print("Parquet file reader factory")
        return ParquetDatasource(file_path)
    elif data_type == "delta":
        print("Delta table reader factory")
        return DeltaDatasource(file_path)
    else:
        raise ValueError(f"Data type: {data_type} not implemented")
