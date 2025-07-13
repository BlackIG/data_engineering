# Databricks notebook source
class DataSink:
    """
    Abstract class
    """
    def __init__(self, df, path, method, params=None):
        self.df = df
        self.path = path
        self.method = method
        self.params = params 

    def load_data_frame(self):
        """
        Abstract method. Function will be define in sub classes
        """
        raise ValueError("Not Implemented")

class SaveToDeltaDataSink(DataSink):

    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)
        print(f"Dataframe saved to lake: {self.path} without partition")

class SaveToDeltaDataSinkWithPartition(DataSink):

    def load_data_frame(self):
        # define a list of columns to partition by
        partitionByColumns = self.params.get("partitionByColumns")

        # write the data frame to delta
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)
        print(f"Dataframe saved to lake: {self.path} with partition")

class SaveToDeltaTable(DataSink):
    def load_data_frame(self):
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)
        print(f"Dataframe saved to table: {self.path} without partition")

def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "delta":
        return SaveToDeltaDataSink(df, path, method, params)
    elif sink_type == "delta_partition":
        return SaveToDeltaDataSinkWithPartition(df, path, method, params)
    elif sink_type == "delta_table":
        return SaveToDeltaTable(df, path, method, params)
    else:
        raise ValueError(f"Not implemented for sink type: {sink_type}")
    