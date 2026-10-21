from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, initcap, upper, when, lit, expr
from datetime import datetime

# Step 1: Temporary view with transformations (source for CDC flow)
@dp.temporary_view()
def product_catalog_changes():
    source_df = spark.readStream.format("delta").option("readChangeFeed", "true").table("retail_q.postgres_bronze.product_catalog")
    return (
        source_df.select(
            upper(trim(col("product_id"))).alias("product_id"),
            initcap(trim(col("product_name"))).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("subcategory")).alias("subcategory"),
            trim(col("brand")).alias("brand"),
            col("unit_price"),
            initcap(trim(col("supplier_name"))).alias("supplier_name"),
            col("launch_date"),
            when(col('unit_price') > 50000, 'Premium')
            .when(col('unit_price') > 10000, 'High')
            .when(col('unit_price') > 5000, 'Medium')
            .when(col('unit_price') > 1000, 'Low')
            .otherwise('Basic').alias('price_segment'),
            col("updated_at"),
            col("_change_type"),
            lit(datetime.now()).cast("timestamp").alias("processed_at"),
        )
    )

# Step 2: Create target streaming table with data quality expectations
dp.create_streaming_table(
    name="retail_q.retail_silver.product_catalog",
    comment="Silver layer product catalog with standardized fields and data quality checks",
    cluster_by=["product_id"],
    expect_all_or_fail={"valid_product_id": "product_id IS NOT NULL"},
    expect_all_or_drop={
        "valid_product_name": "product_name IS NOT NULL",
        "valid_category": "category IS NOT NULL",
        "valid_brand": "brand IS NOT NULL",
    },
    expect_all={
        "valid_unit_price": "unit_price >= 0",
        "valid_launch_date": "launch_date IS NOT NULL",
    },
)

# Step 3: Apply CDC flow — upsert by product_id, sequenced by updated_at (SCD2: history tracking)
dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.product_catalog",
    source="product_catalog_changes",
    keys=["product_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)
