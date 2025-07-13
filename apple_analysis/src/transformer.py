# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains, row_number, when, lower, lead, sum, desc, unix_timestamp, avg, round

class Transformer:
  def __init__(self):
      pass
  
  def transform(self, inputDFs):
      pass
  
class AirpodsAfterIphoneTransformer(Transformer):
    """
        Customers who bought Airpods after buying iPhone
        inputDFs : A dictionary
    """
    def transform(self, inputDFs):

        # 1 -- Process transaction from csv source
        transactionInputDF = inputDFs.get("transactionInputDF").withColumn("transaction_date", col("transaction_date").cast("timestamp"))

        # Partition by customer_id, order in asc order of date and use lead to find the next product bought
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        #withColumn creates new column which has the next product bought
        transformedDF = transactionInputDF.withColumn("next_product_name", lead("product_name").over(windowSpec))

        # Sort by customer_id and transaction_date
        # In Spark, transformations are lazy and do not change the original DataFrame unless reassigned.
        transformedDF = transformedDF.orderBy("customer_id", "transaction_date")

        print("transactionInputDF in transform factory")
        transformedDF.show()

        # Filter for customers who bought iPhone and next product bought is AirPods
        filteredDF = transformedDF.filter((col("product_name") == "iPhone") & (col("next_product_name") == "AirPods"))

        print("AirpodsAfterIphoneTransformer Transactions: filteredDF in transformer")
        filteredDF = filteredDF.orderBy("customer_id", "transaction_date", "product_name")
        filteredDF.show()

        # 2 -- Process customer from delta table source
        customerInputDF = inputDFs.get("customerInputDF")

        #Using broadcast join since filteredDF is a smaller dataset and can fit in memory -- join on customer_id
        joinedDF = customerInputDF.join(
            broadcast(filteredDF),"customer_id"
            )
        
        print("AirpodsAfterIphoneTransformer: joinedDF - Transactions with customerInputDF in transformer")
        joinedDF = joinedDF.orderBy("customer_id", "transaction_date", "product_name")
        joinedDF.show()

        # Return the joinedDF, select specific columns
        return joinedDF.select("customer_id", "customer_name", "location")

class OnlyAirpodsAndIphoneTransformer(Transformer):
    """
    Customers who bought only Airpods and iPhone
            inputDFs : A dictionary
    """
    def transform(self, inputDFs):
        # 1 -- Process transaction from csv source
        transactionInputDF = inputDFs.get("transactionInputDF").withColumn("transaction_date", col("transaction_date").cast("timestamp"))

        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
            )
        groupedDF.show()

        filteredDF = groupedDF.filter(size("products") == 2)

        filteredDF = filteredDF.filter(array_contains("products", "AirPods") & array_contains("products", "iPhone"))

        filteredDF = filteredDF.orderBy("customer_id")
        print("OnlyAirpodsAndIphone: filteredDF in transformer")
        filteredDF.show()
                                      
        # 2 -- Process customer from delta table source
        customerInputDF = inputDFs.get("customerInputDF")

        #Using broadcast join since filteredDF is a smaller dataset and can fit in memory -- join on customer_id
        joinedDF = customerInputDF.join(
            broadcast(filteredDF),"customer_id"
            )
        
        print("OnlyAirpodsAndIphone: joinedDF - with customerInputDF in transformer")
        joinedDF = joinedDF.orderBy("customer_id")

        joinedDF.show()

        # Return the joinedDF, select specific columns
        return joinedDF.select("customer_id", "customer_name", "location")



class ProductsAfterFirstPurchaseTransformer(Transformer):
    """
    List all products bought by customers after their initial purchase 
    """ 
    def transform(self, inputDFs):
        
        # 1 -- Process transaction from csv source
        transactionInputDF = inputDFs.get("transactionInputDF").withColumn("transaction_date", col("transaction_date").cast("timestamp"))

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        
        df_ranked = transactionInputDF.withColumn(
            "rank", row_number().over(windowSpec))
        
        # Get the first purchase date for each customer
        df_rank1 = df_ranked.filter(col("rank") == 1).withColumnRenamed("product_name", "first_product").orderBy("customer_id")

        # Get purchases after the first purchase
        df_rank_others = df_ranked.filter(col("rank") > 1)

        df_agg =df_rank_others.groupBy("customer_id").agg(
            collect_set("product_name").alias("products_after_first_purchase"))
        
        df_agg = df_agg.orderBy("customer_id")

        print("ProductsAfterFirstPurchaseTransformer in transformer")

        # broadcast join df_agg to df_rank1
        firstPurchaseDF = df_rank1.join(
            broadcast(df_agg), "customer_id"
        )

        # 2 -- Process customer from delta table source
        customerInputDF = inputDFs.get("customerInputDF")
        print("customerInputDF in transform factory")
        customerInputDF.show()

        #Using broadcast join since firstPurchaseDF is a smaller dataset and can fit in memory -- join on customer_id
        joinedDF = customerInputDF.join(
            broadcast(firstPurchaseDF),"customer_id"
            )
        
        print("ProductsAfterFirstPurchase: joinedDF - with customerInputDF in transformer")
        joinedDF = joinedDF.orderBy("customer_id")

        joinedDF.show()

        # Return the joinedDF, select specific columns
        return joinedDF.select("customer_id", "customer_name", "location", "first_product", "products_after_first_purchase")

class TopThreeRevenueProductsPerCategoryTransformer(Transformer):
    """
    Identify top three selling products in each category by total revenue
    """
    def transform(self, inputDFs):

        # 1 -- Process transaction from csv source
        transactionInputDF = inputDFs.get("transactionInputDF").withColumn("transaction_date", col("transaction_date").cast("timestamp"))

        # 2 -- Process product from parquet source
        productDF = inputDFs.get("productInputDF")

        # Renaming product_name to p_product_name to prevent confusion with same column name in transaction table
        productDF = productDF.withColumnRenamed("product_name", "p_product_name")

        print("productDF: product_name renamed to p_product_name in transform factory")
        productDF.show()

        # Step 1: Normalize product_name to lowercase 
        transaction_df_with_product_id = transactionInputDF.withColumn("product_name_lower", lower(col("product_name")))

        # Step 2: Add product_id based on product_name in products table. I observed the pattern between both dataframes
        transaction_df_with_product_id = transaction_df_with_product_id.withColumn(
            "product_id", 
            when(col("product_name_lower") == "iphone", 5)
            .when(col("product_name_lower") == "airpods", 6)
            .when(col("product_name_lower") == "macbook", 7)
            .when(col("product_name_lower") == "ipad", 8)
            .otherwise(None)
        ).orderBy("product_id")
        transaction_df_with_product_id.show()

        # broadcast join product_df (smaller dataset) to transaction_df_with_product_id
        joinedDF = transaction_df_with_product_id.join(
            broadcast(productDF), "product_id"
            )
        # Step 3: Calculate total revenue per product per category
        revenueDF = joinedDF.groupBy("product_name","category").agg(sum("price").alias("total_revenue")).orderBy(desc("total_revenue"))

        # Step 4: Rank products per category by total revenue
        windowSpec = Window.partitionBy("category").orderBy(desc("total_revenue"))

        rankedDF = revenueDF.withColumn("rank", row_number().over(windowSpec))

        # Step 5: Filter top 3 per category
        topCategoryDF = rankedDF.filter(col("rank") <= 3).select("category", "product_name", "total_revenue")

        print("TopThreeRevenueProductsPerCategoryTransformer in transformer")
        topCategoryDF.show()

        return topCategoryDF

class DelayBetweenIphoneAndAirpodPurchaseTransformer(Transformer):
    """
    Determine the average time between purchase of iPhone and AirPods
    """
    def transform(self, inputDFs):

        # 1 -- Process transaction from csv source
        transactionInputDF = inputDFs.get("transactionInputDF").withColumn("transaction_date", col("transaction_date").cast("timestamp"))

        # 2 -- Process customer from delta table source
        customerInputDF = inputDFs.get("customerInputDF")
        
        # Step 1: Normalize product_name and filter for relevant products
        transactionInputDF = transactionInputDF.withColumn(
            "product_name_lower", lower(col("product_name")))
            
        transactionDF = transactionInputDF.filter(
            (col("product_name_lower") == "iphone") |
            (col("product_name_lower") == "airpods") )


        # Step 2: Partition by customer, order by transaction date
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        # Step 3: Use lead() to get the next product and next date
        rankedDF = transactionDF.withColumn("next_product", lead("product_name_lower").over(windowSpec)) \
                                .withColumn("next_date", lead("transaction_date").over(windowSpec))

        # Step 4: Filter where current row is iPhone, next row is Airpods
        iphone_airpods_pairs = rankedDF.filter(
            (col("product_name_lower") == "iphone") & (col("next_product") == "airpods")
        )
        print("iphone_airpods_pairs")
        iphone_airpods_pairs.show()

        # Step 5: Compute time difference
        pairedDF = iphone_airpods_pairs.withColumn(
            "time_diff_seconds",
            unix_timestamp("next_date") - unix_timestamp("transaction_date")
        )

        # Step 6: Average time difference per customer
        groupedDF = pairedDF.groupBy("customer_id").agg(
            avg("time_diff_seconds").alias("avg_secs_between_purchase")
        )

        groupedDF = groupedDF.withColumn(
            "avg_days_between_purchase",
            round(col("avg_secs_between_purchase") / 86400, 2)
        )
        print("groupedDF")
        groupedDF.show()

        #Using broadcast join since groupedDF is a smaller dataset and can fit in memory -- join on customer_id
        joinedDF = customerInputDF.join(
            broadcast(groupedDF),"customer_id"
            )
        
        print("DelayBetweenIphoneAndAirpodPurchaseTransformer: joinedDF - with customerInputDF in transformer")
        joinedDF = joinedDF.orderBy("customer_id")
        joinedDF.show()

        # Return the joinedDF, select specific columns
        return joinedDF.select("customer_id", "customer_name", "location", "avg_secs_between_purchase", "avg_days_between_purchase")