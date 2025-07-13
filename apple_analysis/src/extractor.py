# Databricks notebook source
# MAGIC %run  "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class for extractors
    -- data is a list of strings which are categories of data to be returned e.g ["transaction", "customer", "product"]
    """
    def __init__(self, data):
        self.data = data

    def extract(self):
        return "extracted"
    

class DataFrameExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        if not self.data:
            raise Exception("No data provided")

        inputDFs = {}

        if "transaction" in self.data:
            transactionInputDF = get_data_source(
                data_type="csv",
                file_path="/Volumes/workspace/apple_analysis/source_filestore/Transaction_Updated.csv"
            ).get_data_frame()
            transactionInputDF = transactionInputDF.orderBy("customer_id", "transaction_date")
            print("Extractor: transactionInputDF")
            transactionInputDF.show()
            inputDFs["transactionInputDF"] = transactionInputDF

        if "customer" in self.data:
            customerInputDF = get_data_source(
                data_type="delta",
                file_path="workspace.apple_analysis.customer_updated"
            ).get_data_frame()
            customerInputDF = customerInputDF.orderBy("customer_id")
            print("Extractor: customerInputDF")
            customerInputDF.show()
            inputDFs["customerInputDF"] = customerInputDF
        
        if "product" in self.data:
            productInputDF = get_data_source(
                data_type="parquet",
                file_path = "/Volumes/workspace/apple_analysis/source_filestore/Products_Updated_Parquet.parquet"
            ).get_data_frame()
            productInputDF = productInputDF.orderBy("product_id")
            print("Extractor: productInputDF")
            productInputDF.show()
            inputDFs["productInputDF"] = productInputDF
            

        return inputDFs
