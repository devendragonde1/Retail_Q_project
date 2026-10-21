from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime

# Step 1: Temporary view with transformations (source for CDC flow)
@dp.temporary_view()
def inventory_changes():
    source_df = spark.readStream.format("delta").option("readChangeFeed", "true").table('retail_q.postgres_bronze.inventory')
    return source_df.select(
        F.col("inventory_id"),
        F.col("product_id"),
        F.col("store_id"),
        F.col("stock_quantity"),
        F.col("reorder_level"),
        F.when(
            F.col("stock_quantity") < F.col("reorder_level"),
            "low"
        ).otherwise("ok").alias("inventory_status"),
        F.col("warehouse_location"),
        F.col("Last_stock_update"),
        F.col("_change_type"),
        F.lit(datetime.now()).cast("timestamp").alias("processed_at"),
    )

# Step 2: Create target streaming table with data quality expectations
dp.create_streaming_table(
    name="retail_q.retail_silver.inventory",
    comment="inventory data with profiled columns and data quality checks",
    cluster_by=["inventory_id"],
    expect_all_or_drop={"non-null inventory_id": "inventory_id is not null"},
    expect_all={
        "Valid Stock quantity": "stock_quantity >=0",
        "non-null product_id": "product_id is not null",
        "non-null store id": "store_id is not null",
    },
)

# Step 3: Apply CDC flow — upsert by inventory_id, sequenced by Last_stock_update (SCD1: latest wins)
dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.inventory",
    source="inventory_changes",
    keys=["inventory_id"],
    sequence_by="Last_stock_update",
    stored_as_scd_type=1,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)