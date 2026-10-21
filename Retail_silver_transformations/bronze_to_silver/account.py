from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime

# Step 1: Temporary view with transformations (source for CDC flow)
@dp.temporary_view()
def account_changes():
    source_df = spark.readStream.format("delta").option("readChangeFeed", "true").table("retail_q.salesforce_bronze.account")
    return source_df.select(
        F.col("Id").alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.upper(F.trim(F.col("Name"))).alias("customer_name"),
        F.col("Type").alias("type"),
        F.col("ParentId").alias("parent_id"),
        F.col("BillingStreet").alias("billing_street"),
        F.col("BillingCity").alias("billing_city"),
        F.col("BillingState").alias("billing_state"),
        F.col("BillingPostalCode").alias("billing_postal_code"),
        F.col("BillingCountry").alias("billing_country"),
        F.col("ShippingStreet").alias("shipping_street"),
        F.col("ShippingCity").alias("shipping_city"),
        F.col("ShippingState").alias("shipping_state"),
        F.col("ShippingPostalCode").alias("shipping_postal_code"),
        F.col("ShippingCountry").alias("shipping_country"),
        F.col("Phone").alias("phone"),
        F.col("Website").alias("website"),
        F.coalesce(F.col("Industry"), F.lit("UNKNOWN")).alias("industry"),
        F.col("AnnualRevenue").alias("annual_revenue"),
        F.col("NumberOfEmployees").alias("number_of_employees"),
        F.col("Description").alias("description"),
        F.col("SystemModstamp"),
        F.col("_change_type"),
        F.lit(datetime.now()).cast("timestamp").alias("processed_at"),
    )

# Step 2: Create target streaming table with data quality expectations
dp.create_streaming_table(
    name="retail_q.retail_silver.account",
    comment="Salesforce account data with core business columns and data quality checks",
    expect_all_or_drop={"non-null id": "id IS NOT NULL"},
    cluster_by=["id"],
    expect_all={"non-null name": "customer_name IS NOT NULL"},
)

# Step 3: Apply CDC flow — upsert by id, sequenced by SystemModstamp (SCD2: history tracking)
dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.account",
    source="account_changes",
    keys=["id"],
    sequence_by="SystemModstamp",
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type"],
)