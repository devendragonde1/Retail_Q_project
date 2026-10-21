from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime

@dp.table(
    name="retail_q.retail_silver.transactions",
    comment="Transactions table with profiled columns and DQ checks"
)

@dp.expect_or_drop('non-null transaction is',"transaction_id is not null")
@dp.expect("valid quantity","quantity>0")
@dp.expect("valid selling price","selling_price>=0")
@dp.expect("valid discount amount","discount_amount>=0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("valid payment_mode",
           "payment_mode IN ('UPI','Card','Cash','Net Banking')")
def transactions_clean():
    source_df=spark.readStream.table("retail_q.blob_bronze.transactions")

    return source_df.select(
                  F.col("transaction_id"),
        F.col("opportunity_name"),
        F.col("product_id"),
        F.col("store_id"),
        F.col("quantity").cast("int"),
        F.col("selling_price").cast("int"),
        (F.col("quantity").cast("int") * F.col("selling_price").cast("int")).alias("gross_amount"),
        F.col("discount_amount").cast("int"),
        F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
        F.col("payment_mode"),
        F.col("sales_channel"),
        F.lit(datetime.now()).cast("timestamp").alias("processed_at")
    )