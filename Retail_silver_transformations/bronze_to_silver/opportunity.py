from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime

# Step 1: Temporary view with transformations (source for CDC flow)
@dp.temporary_view()
def opportunity_changes():
    source_df = spark.readStream.format("delta").option("readChangeFeed", "true").table("retail_q.salesforce_bronze.opportunity")
    return source_df.select(
        F.col("Id").alias("id"),
        F.col("IsDeleted").alias("is_deleted"),
        F.col("AccountId").alias("account_id"),
        F.col("Name").alias("name"),
        F.col("Description").alias("description"),
        F.col("StageName").alias("stage_name"),
        F.col("Amount").alias("amount"),
        F.when(F.col("amount") > 100000, "ENTERPRISE")
         .when(F.col("amount") > 25000, "MID_MARKET")
         .otherwise("SMALL")
         .alias("deal_size"),
        F.col("Probability").alias("probability"),
        F.col("CloseDate").alias("close_date"),
        F.col("Type").alias("type"),
        F.col("NextStep").alias("next_step"),
        F.col("LeadSource").alias("lead_source"),
        F.col("IsClosed").alias("is_closed"),
        F.col("IsWon").alias("is_won"),
        F.col("ForecastCategory").alias("forecast_category"),
        F.col("OwnerId").alias("owner_id"),
        F.col("CreatedDate").alias("created_date"),
        F.col("LastModifiedDate").alias("last_modified_date"),
        F.col("SystemModstamp"),
        F.col("_change_type"),
        F.lit(datetime.now()).cast("timestamp").alias("processed_at"),
    )

# Step 2: Create target streaming table with data quality expectations
dp.create_streaming_table(
    name="retail_q.retail_silver.opportunity",
    cluster_by=["id"],
    comment="Salesforce opportunity data with core sales fields and data quality checks",
    expect_all_or_drop={"non-null id": "id IS NOT NULL"},
    expect_all={
        "non-null name": "name IS NOT NULL",
        "valid amount": "amount IS NULL OR amount >= 0",
        "valid probability": "probability IS NULL OR (probability >= 0 AND probability <= 100)",
        "valid stage": "stage_name IN ('Prospecting','Closed Won','Closed Lost')",
    },
)

# Step 3: Apply CDC flow — upsert by id, sequenced by SystemModstamp (SCD1: latest wins)
dp.create_auto_cdc_flow(
    target="retail_q.retail_silver.opportunity",
    source="opportunity_changes",
    keys=["id"],
    sequence_by="last_modified_date",
    stored_as_scd_type=1,
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["last_modified_date", "_change_type"],
)