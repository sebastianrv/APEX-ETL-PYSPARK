# Importar librerias y dependencias
from pyspark.sql import SparkSession
from omegaconf import OmegaConf
import logging

import read_data
import transform_data
import load_data

# Configurar logging para ver puntos de control en la ejecución
logging.getLogger().setLevel(logging.INFO)


# Ejecutar en orden el ETL
config = OmegaConf.load("config/config.yaml")
spark = SparkSession.builder.appName(config.app.name).getOrCreate()
logging.info("Fase 1: Configurar")

df_ingest = read_data.run(spark, config)
logging.info("Fase 2: Extraer")

df_transform = transform_data.run(df_ingest, config)
logging.info("Fase 3: Transformar")

load_data.run(df_transform, config)
logging.info("Fase 4: Cargar")