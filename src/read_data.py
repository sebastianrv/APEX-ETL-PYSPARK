# Importar sólo la librería necesaria
from pyspark.sql.functions import col


def run(spark, config):
    start_date = config.filters.start_date.replace("-", "")
    end_date = config.filters.end_date.replace("-", "")

    # Lectura del archivo fuente y aplicar filtro parametrizado de fecha y país
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(config.input.path)
        .withColumn("fecha_proceso", col("fecha_proceso").cast("string"))
        .filter(
            (col("fecha_proceso").between(start_date, end_date)) &
            (col("pais") == config.filters.country)
        )
    )
