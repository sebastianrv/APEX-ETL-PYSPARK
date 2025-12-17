# Importar las libreriías necesarias
from pyspark.sql.functions import col, when
from pyspark.sql.types import DecimalType

def run(df, config):
    # Eliminar duplicados
    df = df.dropDuplicates()
    # Transformar cajas a unidades
    df = df.withColumn(
        "cantidad_unidades",
        when(
            col("unidad") == "CS",
            col("cantidad") * config.units.cs_to_units
        ).when(
            col("unidad") == "ST",
            col("cantidad")
        ).otherwise(None)
    )
    df = df.filter(
    col("tipo_entrega").isin("ZPRE", "ZVE1", "Z04", "Z05")
    )
    # Clasificar entregas
    df = (
        df
        .withColumn(
            "entrega_rutina",
            col("tipo_entrega").isin("ZPRE", "ZVE1")
        )
        .withColumn(
            "entrega_bonificacion",
            col("tipo_entrega").isin("Z04", "Z05")
        )
    )

    df = df.withColumn(
        "precio",
        when(col("precio") > 0, col("precio"))
        .cast(DecimalType(10, 2))
    )

    # Limpieza bde valores
    df = (
        df
        .dropna(subset=["cantidad_unidades"])
        .filter(col("cantidad_unidades") > 0)
    )

    return df