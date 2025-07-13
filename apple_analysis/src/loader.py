# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDf):
        self.transformedDf = transformedDf

    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):

        get_sink_source(
            sink_type = "delta", 
            df=self.transformedDf, 
            path="/Volumes/workspace/apple_analysis/datalake/airpods_after_iphone",
            method = "overwrite").load_data_frame()
        
class OnlyAirpodsAndIphoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        
        # Write to data lake
        get_sink_source(
            sink_type = "delta_partition", 
            df=self.transformedDf, 
            path="/Volumes/workspace/apple_analysis/datalake/only_airpods_and_iphone", 
            method = "overwrite", 
            params = params).load_data_frame()
        
        # Write also to delta table - no partition
        get_sink_source(
            sink_type = "delta_table", 
            df=self.transformedDf, 
            path="workspace.apple_analysis.only_airpods_and_iphone_delta", method = "overwrite", 
            params = params).load_data_frame()

class ProductsAfterFirstPurchaseLoader(AbstractLoader):
    def sink(self):

        get_sink_source(
            sink_type = "delta", 
            df=self.transformedDf, 
            path="/Volumes/workspace/apple_analysis/datalake/products_after_first_purchase",
            method = "overwrite").load_data_frame()
        
class TopThreeRevenueProductsPerCategoryLoader(AbstractLoader):
    def sink(self):

        get_sink_source(
            sink_type = "delta", 
            df=self.transformedDf, 
            path="/Volumes/workspace/apple_analysis/datalake/top_three_revenue_products_per_category",
            method = "overwrite").load_data_frame()
        
class DelayBetweenIphoneAndAirpodPurchaseLoader(AbstractLoader):
    def sink(self):
        params =   {"partitionByColumns": ["location"]}
        
        # Write to data lake
        get_sink_source(
            sink_type = "delta_partition", 
            df=self.transformedDf, 
            path="/Volumes/workspace/apple_analysis/datalake/delay_between_iphone_and_airpod_purchase", 
            method = "overwrite", 
            params = params).load_data_frame()
        
        # Write also to delta table - no partition
        get_sink_source(
            sink_type = "delta_table", 
            df=self.transformedDf, 
            path="workspace.apple_analysis.delay_between_iphone_and_airpod_purchase_delta", method = "overwrite", 
            params = params).load_data_frame()