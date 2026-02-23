"""
PostgreSQL to OneLake (Spark) data type mapping utility.

Maps PostgreSQL column data types to their equivalent PySpark / OneLake
Spark data types so that a schema can be constructed programmatically
before loading data into a Microsoft Fabric Lakehouse.
"""

from typing import Callable, Union

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Core mapping: PostgreSQL base type → Spark DataType
# ---------------------------------------------------------------------------

# Entries that need precision/scale carry a *callable* so that the caller can
# pass the actual precision/scale from the information_schema.
PGSQL_TO_SPARK_TYPE_MAP: dict[str, Union[DataType, Callable[..., DataType]]] = {
    # --- Integer family ---
    "smallint": ShortType(),
    "int2": ShortType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "int4": IntegerType(),
    "bigint": LongType(),
    "int8": LongType(),
    "serial": IntegerType(),
    "bigserial": LongType(),
    "smallserial": ShortType(),
    # --- Fixed-point / arbitrary-precision ---
    # Use lambda so callers can pass (precision, scale)
    "numeric": lambda p=38, s=18: DecimalType(p, s),
    "decimal": lambda p=38, s=18: DecimalType(p, s),
    "money": DoubleType(),
    # --- Floating-point ---
    "real": FloatType(),
    "float4": FloatType(),
    "double precision": DoubleType(),
    "float8": DoubleType(),
    "float": DoubleType(),
    # --- Character ---
    "character varying": StringType(),
    "varchar": StringType(),
    "character": StringType(),
    "char": StringType(),
    "bpchar": StringType(),
    "text": StringType(),
    "name": StringType(),
    "citext": StringType(),
    # --- Binary ---
    "bytea": BinaryType(),
    # --- Boolean ---
    "boolean": BooleanType(),
    "bool": BooleanType(),
    # --- Date/time ---
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamp without time zone": TimestampType(),
    "timestamp with time zone": TimestampType(),
    "timestamptz": TimestampType(),
    # Spark has no native interval/time; map to string to preserve value
    "time": StringType(),
    "time without time zone": StringType(),
    "time with time zone": StringType(),
    "timetz": StringType(),
    "interval": StringType(),
    # --- Networking ---
    "inet": StringType(),
    "cidr": StringType(),
    "macaddr": StringType(),
    "macaddr8": StringType(),
    # --- JSON ---
    "json": StringType(),
    "jsonb": StringType(),
    # --- UUID ---
    "uuid": StringType(),
    # --- XML ---
    "xml": StringType(),
    # --- Bit strings ---
    "bit": StringType(),
    "bit varying": StringType(),
    "varbit": StringType(),
    # --- Geometric / range / other --- (fall back to string)
    "point": StringType(),
    "line": StringType(),
    "lseg": StringType(),
    "box": StringType(),
    "path": StringType(),
    "polygon": StringType(),
    "circle": StringType(),
    "int4range": StringType(),
    "int8range": StringType(),
    "numrange": StringType(),
    "tsrange": StringType(),
    "tstzrange": StringType(),
    "daterange": StringType(),
    "tsvector": StringType(),
    "tsquery": StringType(),
    "pg_lsn": StringType(),
    "txid_snapshot": StringType(),
    "oid": LongType(),
}


def map_pgsql_type_to_spark(
    pg_type: str,
    precision: int | None = None,
    scale: int | None = None,
    array_dimensions: int = 0,
) -> DataType:
    """
    Convert a PostgreSQL data type name to a PySpark DataType instance.

    Parameters
    ----------
    pg_type : str
        The PostgreSQL data type as returned by ``information_schema.columns``
        (e.g. ``"integer"``, ``"character varying"``, ``"numeric"``).
        Leading/trailing whitespace and upper-case letters are handled
        automatically.
    precision : int, optional
        Numeric precision (for ``numeric`` / ``decimal`` columns).
    scale : int, optional
        Numeric scale (for ``numeric`` / ``decimal`` columns).
    array_dimensions : int, optional
        Number of array dimensions (``> 0`` for PostgreSQL array types).
        When ``> 0`` the innermost element type is wrapped in
        ``ArrayType`` as many times as there are dimensions.

    Returns
    -------
    pyspark.sql.types.DataType
        Corresponding Spark DataType instance.

    Raises
    ------
    ValueError
        If *pg_type* is unknown and has no default mapping.
    """
    normalised = pg_type.strip().lower()

    # Strip trailing [] that PostgreSQL appends for array element types
    # e.g. "integer[]" → "integer",  array_dimensions bumped automatically
    while normalised.endswith("[]"):
        normalised = normalised[:-2].strip()
        array_dimensions += 1

    # Also handle the "ARRAY" data_type with udt_name as the element type
    if normalised == "array":
        # Caller is expected to pass the element type via *pg_type* after
        # parsing udt_name; fall back to StringType for unknown arrays.
        element_type = StringType()
        if array_dimensions == 0:
            array_dimensions = 1
        spark_type = element_type
        for _ in range(array_dimensions):
            spark_type = ArrayType(spark_type)
        return spark_type

    if normalised not in PGSQL_TO_SPARK_TYPE_MAP:
        # Unknown type — default to StringType with a warning
        import warnings
        warnings.warn(
            f"Unknown PostgreSQL type '{pg_type}'; defaulting to StringType.",
            stacklevel=2,
        )
        spark_type = StringType()
    else:
        entry = PGSQL_TO_SPARK_TYPE_MAP[normalised]
        if callable(entry):
            # Numeric/decimal with precision and scale
            p = precision if precision is not None else 38
            s = scale if scale is not None else 18
            spark_type = entry(p, s)
        else:
            spark_type = entry

    # Wrap in ArrayType for each array dimension
    for _ in range(array_dimensions):
        spark_type = ArrayType(spark_type)

    return spark_type


def build_spark_schema(columns: list[dict]) -> StructType:
    """
    Build a PySpark ``StructType`` from a list of column descriptors.

    Each descriptor is a ``dict`` with at minimum a ``"column_name"`` and
    ``"data_type"`` key — the same shape returned by querying
    ``information_schema.columns``.  Optional keys are ``"numeric_precision"``,
    ``"numeric_scale"``, ``"is_nullable"``, and ``"array_dimensions"``.

    Parameters
    ----------
    columns : list[dict]
        Column descriptors, e.g.::

            [
                {"column_name": "id",    "data_type": "integer",  "is_nullable": "NO"},
                {"column_name": "price", "data_type": "numeric",
                 "numeric_precision": 10, "numeric_scale": 2},
            ]

    Returns
    -------
    pyspark.sql.types.StructType
    """
    fields = []
    for col in columns:
        col_name = col["column_name"]
        pg_type = col["data_type"]
        precision = col.get("numeric_precision")
        scale = col.get("numeric_scale")
        nullable = col.get("is_nullable", "YES").upper() != "NO"
        array_dimensions = int(col.get("array_dimensions", 0))

        spark_type = map_pgsql_type_to_spark(
            pg_type,
            precision=precision,
            scale=scale,
            array_dimensions=array_dimensions,
        )
        fields.append(StructField(col_name, spark_type, nullable))

    return StructType(fields)
