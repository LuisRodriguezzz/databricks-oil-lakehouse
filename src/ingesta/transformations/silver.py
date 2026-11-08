"""Silver: el estado vigente de cada fuente, tipado y validado, más su cuarentena.

Por cada fuente se definen tres objetos:

- `<fuente>_validada`: privada, no se publica. Toma la última entrega, tipa, deja una versión
  por clave y anota en `_motivos` qué reglas no cumple cada fila. Las reglas también se
  registran como expectations, así sus conteos aparecen en la interfaz del pipeline.
- `silver.<fuente>`: las filas sin motivos, con sus tipos. Es lo que lee gold.
- `silver.<fuente>_cuarentena`: las filas con motivos, con el texto original, para auditar.

Son materialized views: en cada corrida quedan iguales a lo que dice bronze, aunque una entrega
corrija o saque filas. Databricks las recalcula de forma incremental cuando puede.

Al final, `silver.control_calidad` resume cada fuente y frena la corrida si una entrega viene
vacía o con demasiadas filas en cuarentena: así el job no corre gold.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities import contratos
from utilities.silver_comun import (
    LINAJE,
    marcar,
    sin_nulos,
    tipar,
    ultima_entrega,
    una_version_por_clave,
)

BRONZE = spark.conf.get("oil_bronze")  # noqa: F821  (spark lo provee el pipeline)
SILVER = spark.conf.get("oil_silver")  # noqa: F821


def definir_silver(fuente: str, contrato: dict) -> None:
    reglas = {**contrato["reglas"], **contratos.REGLAS_COMUNES}
    columnas = list(contrato["tipos"])

    @dp.materialized_view(
        name=f"{fuente}_validada",
        private=True,
        comment=f"{fuente}: última entrega, tipada, con los motivos de cuarentena de cada fila.",
    )
    @dp.expect_all(sin_nulos(reglas))
    def validada():
        filas = spark.read.table(f"{BRONZE}.{fuente}")  # noqa: F821
        filas = ultima_entrega(filas, contrato["anio_en_el_archivo"])
        filas = tipar(filas, contrato["tipos"])
        filas = una_version_por_clave(filas, contrato["clave"], contrato["version"], columnas)
        return marcar(filas, reglas)

    @dp.materialized_view(name=f"{SILVER}.{fuente}", comment=contrato["descripcion"])
    def vigente():
        return (
            spark.read.table(f"{fuente}_validada")  # noqa: F821
            .filter(F.size("_motivos") == 0)
            .select(*columnas, *LINAJE)
        )

    @dp.materialized_view(
        name=f"{SILVER}.{fuente}_cuarentena",
        comment=(
            f"Filas de {fuente} que no cumplen el contrato: el texto original tal como llegó, "
            "los motivos y el archivo de origen."
        ),
    )
    def cuarentena():
        return (
            spark.read.table(f"{fuente}_validada")  # noqa: F821
            .filter(F.size("_motivos") > 0)
            .select("_motivos", "_no_convierte", "_original.*", *LINAJE)
        )


FUENTES = {"produccion": contratos.PRODUCCION, "padron": contratos.PADRON}

for fuente, contrato in FUENTES.items():
    definir_silver(fuente, contrato)


def resumen(fuente: str, contrato: dict) -> DataFrame:
    """Una fila: cuántas filas vigentes y en cuarentena tiene la fuente, y qué porcentaje es."""
    vigentes = spark.read.table(f"{SILVER}.{fuente}").agg(  # noqa: F821
        F.count(F.lit(1)).alias("filas_vigentes")
    )
    en_cuarentena = spark.read.table(f"{SILVER}.{fuente}_cuarentena").agg(  # noqa: F821
        F.count(F.lit(1)).alias("filas_cuarentena")
    )
    total = F.col("filas_vigentes") + F.col("filas_cuarentena")
    porcentaje = F.when(total > 0, F.round(F.col("filas_cuarentena") * 100 / total, 4))
    return vigentes.crossJoin(en_cuarentena).select(
        F.lit(fuente).alias("fuente"),
        "filas_vigentes",
        "filas_cuarentena",
        F.coalesce(porcentaje, F.lit(0.0)).alias("porcentaje_cuarentena"),
        F.lit(contrato["umbral_cuarentena_pct"]).alias("umbral_porcentaje"),
    )


@dp.materialized_view(
    name=f"{SILVER}.control_calidad",
    comment=(
        "Una fila por fuente con filas vigentes, filas en cuarentena y su porcentaje. Si una "
        "entrega viene vacía o supera el umbral, la corrida falla y el job no corre gold."
    ),
    # Un control que falla va a fallar igual en el próximo intento: sin reintentos del flujo.
    spark_conf={"pipelines.maxFlowRetryAttempts": "0"},
)
@dp.expect_all_or_fail(contratos.CONTROL)
def control_calidad():
    return reduce(DataFrame.unionByName, [resumen(f, c) for f, c in FUENTES.items()])
