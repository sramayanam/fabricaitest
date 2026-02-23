# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "language_info": {
# META     "name": "python"
# META   },
# META   "save_output": true,
# META   "spark_compute": {
# META     "compute_id": "/trident/default"
# META   },
# META   "trident": {
# META     "lakehouse": {
# META       "default_lakehouse": "",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # PostgreSQL → OneLake Pipeline Template
# 
# This Microsoft Fabric Spark notebook:
# 1. Connects to a **PostgreSQL** source database.
# 2. Introspects the schema of the requested table from `information_schema.columns`.
# 3. Maps every PostgreSQL column data type to its OneLake / Spark equivalent using `type_mappings.py`.
# 4. Reads the full table with the mapped schema.
# 5. Writes the result as a **Delta table** into the target OneLake Lakehouse.
# 
# ## Prerequisites
# * A PostgreSQL JDBC driver JAR available to the Spark cluster  
#   *(add it as a library attachment in the Fabric Spark environment)*.
# * `type_mappings.py` uploaded to the Lakehouse `Files/` root or co-located with this notebook.
# * A Fabric **Secret** (Key Vault) or Notebook parameter for the database password.
# 
# ## Parameters
# Use the **Parameterize** toggle in Fabric to set these values at runtime.

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# Pipeline parameters — override these at runtime via Fabric pipeline
# or the 'Parameterize' feature of a Fabric notebook.
# ---------------------------------------------------------------------------

# Source PostgreSQL connection
PG_HOST: str = "localhost"          # e.g. "my-pg-server.postgres.database.azure.com"
PG_PORT: int = 5432
PG_DATABASE: str = "source_db"      # database name to connect to
PG_USER: str = "postgres"
PG_PASSWORD: str = ""               # set via Key Vault / secret — do NOT hard-code
PG_SCHEMA: str = "public"           # PostgreSQL schema (namespace)
PG_TABLE: str = ""                  # table name to migrate

# Target OneLake / Lakehouse
LAKEHOUSE_TABLE: str = ""           # target Delta table name in the Lakehouse
                                    # defaults to PG_TABLE when left blank
WRITE_MODE: str = "overwrite"       # "overwrite" | "append" | "merge"
DELTA_PARTITION_COLS: list = []     # optional list of column names to partition by

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "tags": ["parameters"]
# META }

# MARKDOWN ********************

# ## 1 — Imports & Helpers

# CELL ********************

import sys
import warnings

# Add the Lakehouse Files root to sys.path so type_mappings can be imported.
# Adjust the path if type_mappings.py lives elsewhere.
sys.path.insert(0, "/lakehouse/default/Files")

from type_mappings import build_spark_schema, map_pgsql_type_to_spark  # noqa: E402

from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 — Build JDBC Connection

# CELL ********************

jdbc_url: str = (
    f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    "?sslmode=require"  # recommended for Azure Database for PostgreSQL
)

jdbc_properties: dict = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

print(f"JDBC URL: jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}?sslmode=require")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 — Fetch Column Schema from PostgreSQL `information_schema`

# CELL ********************

# Query information_schema.columns to discover column names and data types.
# We also pull numeric_precision / numeric_scale for DECIMAL columns and
# is_nullable so we can faithfully replicate the schema.
schema_query: str = f"""
    SELECT
        column_name,
        data_type,
        numeric_precision,
        numeric_scale,
        is_nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{PG_SCHEMA}'
      AND table_name   = '{PG_TABLE}'
    ORDER BY ordinal_position
"""

schema_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", schema_query)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

schema_rows: list = schema_df.collect()

if not schema_rows:
    raise ValueError(
        f"No columns found for '{PG_SCHEMA}.{PG_TABLE}' in database '{PG_DATABASE}'. "
        "Check that the table exists and the user has SELECT privileges on information_schema."
    )

print(f"Found {len(schema_rows)} column(s) in {PG_SCHEMA}.{PG_TABLE}:")
schema_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 — Map PostgreSQL Types → Spark / OneLake Types

# CELL ********************

# Convert each Row to a plain dict so build_spark_schema can consume it.
column_descriptors: list[dict] = [
    {
        "column_name":       row["column_name"],
        "data_type":         row["data_type"],
        "numeric_precision": row["numeric_precision"],
        "numeric_scale":     row["numeric_scale"],
        "is_nullable":       row["is_nullable"],
    }
    for row in schema_rows
]

spark_schema = build_spark_schema(column_descriptors)

print("\nMapped Spark schema:")
print(spark_schema.simpleString())

# Pretty-print the per-column mapping for auditing / debugging
print("\n{:<35} {:<35} {}".format("PostgreSQL type", "Spark type", "Column"))
print("-" * 90)
for col_desc, field in zip(column_descriptors, spark_schema.fields):
    print("{:<35} {:<35} {}".format(col_desc["data_type"], str(field.dataType), field.name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 — Read Source Table from PostgreSQL

# CELL ********************

qualified_table: str = f'"{PG_SCHEMA}"."{PG_TABLE}"'

source_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", qualified_table)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    # Parallel read: tune numPartitions, partitionColumn, lowerBound,
    # upperBound for large tables to improve throughput.
    # .option("numPartitions", "8")
    # .option("partitionColumn", "id")
    # .option("lowerBound", "1")
    # .option("upperBound", "1000000")
    .schema(spark_schema)
    .load()
)

print(f"Row count: {source_df.count():,}")
source_df.printSchema()
source_df.show(5, truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 — Write to OneLake Delta Table

# CELL ********************

target_table: str = LAKEHOUSE_TABLE if LAKEHOUSE_TABLE else PG_TABLE

writer = source_df.write.format("delta").mode(WRITE_MODE)

if DELTA_PARTITION_COLS:
    writer = writer.partitionBy(*DELTA_PARTITION_COLS)

writer.saveAsTable(target_table)

print(f"✅  Table '{target_table}' written to OneLake in '{WRITE_MODE}' mode.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 — (Optional) Verify Written Data

# CELL ********************

verification_df = spark.sql(f"SELECT * FROM {target_table} LIMIT 10")
verification_df.show(truncate=True)
print(f"Total rows in OneLake table '{target_table}': {spark.table(target_table).count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
