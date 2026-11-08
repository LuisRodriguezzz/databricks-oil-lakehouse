"""Ayudas para armar filas de bronze en los tests: todo como texto, como lo deja Auto Loader."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utilities.silver_comun import LINAJE

CARPETA = "/Volumes/oil_test/landing/archivos/produccion"


def archivo(fecha: str, nombre: str) -> str:
    """La ruta de un archivo de landing, con su carpeta de entrega."""
    return f"{CARPETA}/ingest_date={fecha}/{nombre}"


def bronze(spark: SparkSession, filas: list[dict], columnas: list[str]) -> DataFrame:
    """Un DataFrame como bronze: las columnas pedidas más el linaje, todas como texto.

    Cada fila es un diccionario; lo que no trae queda nulo. `_archivo` define la entrega e
    `ingest_date` sale de su carpeta, como hace Auto Loader.
    """
    todas = [*columnas, "_rescued_data", *LINAJE]
    datos = []
    for fila in filas:
        fila = dict(fila)
        ruta = fila.get("_archivo", archivo("2026-11-07", "produccion_2024_prueba.csv"))
        fila["_archivo"] = ruta
        fila.setdefault("ingest_date", ruta.split("ingest_date=")[1].split("/")[0])
        datos.append(tuple(fila.get(columna) for columna in todas))
    esquema = StructType([StructField(columna, StringType(), True) for columna in todas])
    return spark.createDataFrame(datos, esquema)
