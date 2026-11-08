"""Bronze: cada archivo de landing tal como llegó, como texto y con su origen.

Auto Loader recuerda qué archivos ya procesó (por su ruta): cada entrega entra una sola vez y
volver a correr el pipeline no duplica nada. Por eso landing es inmutable: una corrección de la
fuente llega como un archivo nuevo, en otra carpeta de fecha, y bronze guarda las dos versiones.
Elegir cuál vale es trabajo de silver.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Viene de la configuración del pipeline (resources/pipeline_ingesta.yml), así el mismo código
# lee el landing de dev o el de prod.
LANDING = spark.conf.get("oil_landing")  # noqa: F821  (spark lo provee el pipeline)

# Algunos CSV del portal traen la marca BOM pegada al nombre de la primera columna.
BOM = "﻿"


def leer_entregas(fuente: str):
    """Todas las entregas de una fuente, de forma incremental, con su linaje."""
    crudo = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # Todo como texto: tipar y validar es trabajo de silver. Para CSV ya es el default; se
        # deja escrito porque es una decisión de diseño, no un descuido.
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{LANDING}/{fuente}/")
    )
    # Primero el linaje (de qué archivo salió cada fila y cuándo entró), después los nombres:
    # `_metadata` es una columna oculta del lector de archivos.
    con_linaje = crudo.select(
        "*",
        F.col("_metadata.file_path").alias("_archivo"),
        F.col("_metadata.file_modification_time").alias("_archivo_modificado_en"),
        F.current_timestamp().alias("_cargado_en"),
    )
    return con_linaje.toDF(*[columna.lstrip(BOM) for columna in con_linaje.columns])


@dp.table(
    name="produccion",
    comment="Declaraciones juradas de producción por pozo y mes, tal como llegaron del portal.",
)
def produccion():
    return leer_entregas("produccion")


@dp.table(
    name="padron",
    comment="Padrón de pozos con su primera producción, tal como llegó del portal.",
)
def padron():
    return leer_entregas("padron")
