# Databricks notebook source
##  Spark session - the entry point of Spark application

# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession object
spark = SparkSession.builder.appName("apple_analysis").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC  To query a table:
# MAGIC <br> customer_df = spark.sql("SELECT * FROM workspace.customer.customer_updated")
# MAGIC <br>customer_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC To load file from a volume: <br>
# MAGIC spark_df = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/my_volumes/apple_analysis_volume/Customer_Updated.csv")
# MAGIC <br>spark_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ...Importing factories created in nother notebooks using magic command %run

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./transformer"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

# MAGIC %run "./logger"

# COMMAND ----------

class WorkflowBase:
    """
    Base class for all workflows
     -- schema for log table is inferred from the initialisation values
    """
    def __init__(self, job_name):
        self.job_name = job_name
        self.job_status = ""
        self.record_count = 0
        self.remarks = ""

    def log_job(self):
        """
        Log job details
        """
        JobLogger(self.job_name).log(
            job_status=self.job_status,
            record_count=self.record_count,
            remarks=self.remarks
        )


class AirpodsAfterIphoneWorkflow(WorkflowBase):
    """
    ETL pipeline for customers who have bought Airpods immediately after iphone
    """
    def __init__(self):
        super().__init__("AirpodsAfterIphoneWorkflow")

    def runner(self):

        print(f"{self.job_name} started")
        
        #Step 1: Extract required data from different sources
        inputDFs = DataFrameExtractor(data=["transaction", "customer"]).extract()

        #Step 2: Implement transformation logic - customers who have bought Airpods immediately after iphone
        airpodsAfterIphoneDF = AirpodsAfterIphoneTransformer().transform(
            inputDFs = inputDFs
        )
        
        #Step 3: Load all required data to different sink if not empty
        if not airpodsAfterIphoneDF.limit(1).count() == 0:
            self.record_count = airpodsAfterIphoneDF.count()
            AirpodsAfterIphoneLoader(airpodsAfterIphoneDF).sink()
            self.job_status = "Complete"
            
        else:
            print("Skipped loading: airpodsAfterIphoneDF is empty.")
            self.remarks = "Skipped loading: airpodsAfterIphoneDF is empty."
            self.job_status = "Skipped"

        print(f"{self.job_name} completed")

        #log job details
        self.log_job()

class OnlyAirpodsAndIphoneWorkflow(WorkflowBase):
    """
    ETL pipeline for customers who have bought both Airpods and iphone and nothing else
    """
    def __init__(self):
        super().__init__("OnlyAirpodsAndIphoneWorkflow")
        
    def runner(self):

        print(f"{self.job_name} started")

        #Step 1: Extract required data from different sources
        inputDFs = DataFrameExtractor(data=["transaction", "customer"]).extract()

        #Step 2: Implement transformation logic - customers who have bought both Airpods and iphone and nothing else
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphoneTransformer().transform(
            inputDFs = inputDFs
        )
        
        #Step 3: Load all required data to different sink if not empty
        if not onlyAirpodsAndIphoneDF.limit(1).count() == 0:
            self.record_count = onlyAirpodsAndIphoneDF.count()
            OnlyAirpodsAndIphoneLoader(onlyAirpodsAndIphoneDF).sink()
            self.job_status = "Complete"
        else:
            print("Skipped loading: onlyAirpodsAndIphoneDF is empty.")
            self.remarks = "Skipped loading: onlyAirpodsAndIphoneDF is empty."
            self.job_status = "Skipped"

        print(f"{self.job_name} completed")

        #log job details
        self.log_job()

class ProductsAfterFirstPurchaseWorkflow(WorkflowBase):
    """
    Pipeline for products bought after first purchase
    """
    def __init__(self):
        super().__init__("ProductsAfterFirstPurchaseWorkflow")

    def runner(self):

        print(f"{self.job_name} started")
        
        #Step 1: Extract required data from different sources
        inputDFs = DataFrameExtractor(data=["transaction", "customer"]).extract()

        #Step 2: Implement transformation logic - products bought after first purchase
        productsAfterFirstPurchaseDF = ProductsAfterFirstPurchaseTransformer().transform(
            inputDFs = inputDFs
        )
        
        #Step 3: Load all required data to different sink if not empty
        if not productsAfterFirstPurchaseDF.limit(1).count() == 0:
            self.record_count = productsAfterFirstPurchaseDF.count()
            ProductsAfterFirstPurchaseLoader(productsAfterFirstPurchaseDF).sink()
            self.job_status = "Complete"
        else:
            print("Skipped loading: productsAfterFirstPurchaseDF is empty.")
            self.remarks = "Skipped loading: productsAfterFirstPurchaseDF is empty."
            self.job_status = "Skipped"

        print(f"{self.job_name} completed")

        #log job details
        self.log_job()
        
class TopThreeRevenueProductsPerCategoryWorkflow(WorkflowBase):
    """
    Pipeline for top three revenue products per category
    """
    def __init__(self):
        super().__init__("TopThreeRevenueProductsPerCategoryWorkflow")

    def runner(self):

        print(f"{self.job_name} started")
        
        #Step 1: Extract required data from different sources
        inputDFs = DataFrameExtractor(data=["transaction", "product"]).extract()

        #Step 2: Implement transformation logic - top three revenue products per category
        topThreeRevenueProductsPerCategoryDF = TopThreeRevenueProductsPerCategoryTransformer().transform(
            inputDFs = inputDFs
        )
        
        #Step 3: Load all required data to different sink if not empty
        if not topThreeRevenueProductsPerCategoryDF.limit(1).count() == 0:
            self.record_count = topThreeRevenueProductsPerCategoryDF.count()
            TopThreeRevenueProductsPerCategoryLoader(topThreeRevenueProductsPerCategoryDF).sink()
            self.job_status = "Complete"
        else:
            print("Skipped loading: TopThreeRevenueProductsPerCategoryDF is empty.")
            self.remarks = "Skipped loading: TopThreeRevenueProductsPerCategoryDF is empty."
            self.job_status = "Skipped"

        print(f"{self.job_name} completed")

        #log job details
        self.log_job()

class DelayBetweenIphoneAndAirpodPurchaseWorkflow(WorkflowBase):
    """
    Pipeline for top three revenue products per category
    """
    def __init__(self):
        super().__init__("DelayBetweenIphoneAndAirpodPurchaseWorkflow")

    def runner(self):

        print(f"{self.job_name} started")
        
        #Step 1: Extract required data from different sources
        inputDFs = DataFrameExtractor(data=["transaction", "customer"]).extract()

        #Step 2: Implement transformation logic - delay between iphone and airpod purchase
        
        delayBetweenIphoneAndAirpodPurchaseDF = DelayBetweenIphoneAndAirpodPurchaseTransformer().transform(
            inputDFs = inputDFs
        )
        
        #Step 3: Load all required data to different sink if not empty
        if not delayBetweenIphoneAndAirpodPurchaseDF.limit(1).count() == 0:
            self.record_count = delayBetweenIphoneAndAirpodPurchaseDF.count()
            DelayBetweenIphoneAndAirpodPurchaseLoader(delayBetweenIphoneAndAirpodPurchaseDF).sink()
            self.job_status = "Complete"
        else: 
            print("Skipped loading: DelayBetweenIphoneAndAirpodPurchaseDF is empty.")
            self.remarks = "Skipped loading: DelayBetweenIphoneAndAirpodPurchaseDF is empty"
            self.job_status = "Skipped"

        print(f"{self.job_name} completed")

        #log job details
        self.log_job()

# COMMAND ----------

class WorkflowRunner:
    
    def __init__(self, name):
        self.name = name
        
    def runner(self):
        if self.name == "airpods_after_iphone":
            return AirpodsAfterIphoneWorkflow().runner()
        elif self.name == "only_airpods_and_iphone":
            return OnlyAirpodsAndIphoneWorkflow().runner()
        elif self.name == "products_after_first_purchase":
            return ProductsAfterFirstPurchaseWorkflow().runner()
        elif self.name == "top_three_revenue_products_per_category":
            return TopThreeRevenueProductsPerCategoryWorkflow().runner()
        elif self.name == "delay_between_iphone_and_airpod_purchase":
            return DelayBetweenIphoneAndAirpodPurchaseWorkflow().runner()
        else:
            raise ValueError(f"WorkflowRunner Not implemented for {self.name}")

# COMMAND ----------

names = ["airpods_after_iphone" , "only_airpods_and_iphone", "top_three_revenue_products_per_category", "products_after_first_purchase", "delay_between_iphone_and_airpod_purchase"]

for name in names:
    WorkflowRunner(name = name).runner()