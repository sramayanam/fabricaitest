# fabricaitest

A collection of **Microsoft Fabric** pipeline templates.

---

## PostgreSQL → OneLake Pipeline

`pipeline/pgsql_to_onelake.ipynb` is a Fabric Spark notebook template that:

1. Connects to a **PostgreSQL** source database via JDBC.
2. Queries `information_schema.columns` to discover column names and data types
   for a given database, schema, and table.
3. Maps every PostgreSQL column type to its **OneLake / Spark** equivalent using
   the mapping table in `pipeline/type_mappings.py`.
4. Reads the full table from PostgreSQL with the inferred Spark schema.
5. Writes the result as a **Delta table** into the target OneLake Lakehouse.

### Files

| File | Description |
|------|-------------|
| `pipeline/pgsql_to_onelake.Notebook/notebook-content.py` | Main Fabric Spark notebook pipeline template (Fabric Git format) |
| `pipeline/pgsql_to_onelake.Notebook/.platform` | Fabric item metadata (type, displayName, logicalId) |
| `pipeline/type_mappings.py` | PostgreSQL → Spark type mapping utility |

### PostgreSQL → Spark type mapping summary

| PostgreSQL type | Spark / OneLake type |
|----------------|----------------------|
| `smallint`, `int2`, `smallserial` | `ShortType` |
| `integer`, `int`, `int4`, `serial` | `IntegerType` |
| `bigint`, `int8`, `bigserial` | `LongType` |
| `numeric(p,s)`, `decimal(p,s)` | `DecimalType(p, s)` |
| `real`, `float4` | `FloatType` |
| `double precision`, `float8`, `float` | `DoubleType` |
| `money` | `DoubleType` |
| `varchar`, `character varying`, `text`, `char`, `name`, `citext` | `StringType` |
| `bytea` | `BinaryType` |
| `boolean`, `bool` | `BooleanType` |
| `date` | `DateType` |
| `timestamp`, `timestamptz`, `timestamp with/without time zone` | `TimestampType` |
| `time`, `timetz`, `interval` | `StringType` |
| `json`, `jsonb`, `xml`, `uuid` | `StringType` |
| `inet`, `cidr`, `macaddr`, `macaddr8` | `StringType` |
| `bit`, `bit varying`, `varbit` | `StringType` |
| `oid` | `LongType` |
| `integer[]` / any `ARRAY` type | `ArrayType(<element type>)` |
| All other / unknown types | `StringType` (with warning) |

### Prerequisites

* **PostgreSQL JDBC driver** JAR attached to the Fabric Spark environment
  (`postgresql-<version>.jar`).
* `type_mappings.py` uploaded to the Lakehouse **Files/** root (or adjust
  `sys.path` in the notebook).
* Database password stored in **Azure Key Vault** and surfaced via a Fabric
  Data Pipeline parameter — do **not** hard-code credentials.

### Quick start

1. Open the Lakehouse in Microsoft Fabric.
2. Upload `pipeline/type_mappings.py` to `Files/`.
3. Import `pipeline/pgsql_to_onelake.Notebook/` as a new Notebook via Fabric Git integration.
4. Set the notebook parameters (either inline or via a Fabric Data Pipeline):

   | Parameter | Example value |
   |-----------|---------------|
   | `PG_HOST` | `my-server.postgres.database.azure.com` |
   | `PG_PORT` | `5432` |
   | `PG_DATABASE` | `source_db` |
   | `PG_USER` | `postgres` |
   | `PG_PASSWORD` | *(from Key Vault)* |
   | `PG_SCHEMA` | `public` |
   | `PG_TABLE` | `orders` |
   | `LAKEHOUSE_TABLE` | `orders` *(defaults to `PG_TABLE`)* |
   | `WRITE_MODE` | `overwrite` |
   | `DELTA_PARTITION_COLS` | `["created_date"]` *(optional)* |

5. Run the notebook. The mapped Delta table will appear in the Lakehouse.
