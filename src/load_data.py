# Importar sólo las librerías necesarias
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType,IntegerType,DecimalType,BooleanType


def run(df, config):
    # Mapeo del nombre del país
    country_map = {
        "GT": "Guatemala",
        "PE": "Peru",
        "EC": "Ecuador",
        "SV": "El Salvador",
        "HN": "Honduras",
        "JM": "Jamaica"
    }
    df = df.replace(country_map, subset=["pais"])
    df = (
    df
    .withColumn("year", col("fecha_proceso").substr(1, 4))
    .withColumn("month", col("fecha_proceso").substr(5, 2))
    )
    # Selección y casteo de columnas
    df = df.select(
        col("pais").cast(StringType()).alias("country"),
        col("material").cast(StringType()).alias("material"),
        col("transporte").cast(StringType()).alias("transport"),
        col("ruta").cast(StringType()).alias("route"),
        col("precio").cast(DecimalType()).alias("price"),
        col("cantidad_unidades").cast(IntegerType()).alias("unit_quantity"),
        col("entrega_rutina").cast(BooleanType()).alias("routine_delivery"),
        col("entrega_bonificacion").cast(BooleanType()).alias("bonus_delivery"),
        col("year").cast(StringType()),
        col("month").cast(StringType()),
        current_timestamp().alias("load_date")
    ).dropna()

    # Escritura en Parquet particionado por fecha
    (
        df
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(config.output.path)
    )