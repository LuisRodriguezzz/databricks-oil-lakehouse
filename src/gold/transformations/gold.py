"""Gold: tablas listas para analizar, calculadas desde silver.

Corre en su propio pipeline (`oil_gold`). Lee silver por nombre completo porque son tablas de
otro pipeline: acá no hay dependencia que deducir, el orden lo pone el job (ingesta antes que
gold, y gold solo si la ingesta terminó bien).

Son materialized views: si silver corrige o saca una fila, gold la sigue en la próxima corrida.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = spark.conf.get("oil_silver")  # noqa: F821  (spark lo provee el pipeline)


@dp.materialized_view(
    name="produccion_pozo_mes",
    comment=(
        "Una fila por pozo y mes declarado, con la edad del pozo (meses desde su primera "
        "producción, según el padrón) y los atributos para filtrar. Volúmenes: petróleo y agua "
        "en m3, gas en miles de m3."
    ),
)
def produccion_pozo_mes():
    produccion = spark.read.table(f"{SILVER}.produccion")  # noqa: F821
    primera = spark.read.table(f"{SILVER}.padron").select(  # noqa: F821
        "idpozo", F.make_date("anio", "mes", F.lit(1)).alias("primera_produccion")
    )
    mes_declarado = F.make_date("anio", "mes", F.lit(1))
    # Left join: un pozo que no está en el padrón conserva su producción, con la edad vacía.
    return produccion.join(primera, "idpozo", "left").select(
        "idpozo",
        "anio",
        "mes",
        mes_declarado.alias("mes_declarado"),
        "idempresa",
        "empresa",
        "areayacimiento",
        "formacion",
        "sub_tipo_recurso",
        "tipopozo",
        "tipoestado",
        "prod_pet",
        "prod_gas",
        "prod_agua",
        "iny_agua",
        "iny_gas",
        "tef",
        "primera_produccion",
        # Edad del pozo: 0 es el mes de su primera producción; negativa si declaró antes.
        F.months_between(mes_declarado, F.col("primera_produccion"))
        .cast("int")
        .alias("meses_desde_primera_produccion"),
    )


@dp.materialized_view(
    name="produccion_mensual_empresa",
    comment=(
        "Producción total por empresa y mes: petróleo (m3), gas (miles de m3), pozos que "
        "declararon y pozos que produjeron petróleo o gas."
    ),
)
def produccion_mensual_empresa():
    produjo = (F.col("prod_pet") > 0) | (F.col("prod_gas") > 0)
    return (
        spark.read.table("produccion_pozo_mes")  # noqa: F821  (tabla de este mismo pipeline)
        .groupBy("idempresa", "empresa", "anio", "mes", "mes_declarado")
        .agg(
            F.sum("prod_pet").alias("petroleo_m3"),
            F.sum("prod_gas").alias("gas_miles_m3"),
            F.countDistinct("idpozo").alias("pozos_declarados"),
            F.countDistinct(F.when(produjo, F.col("idpozo"))).alias("pozos_activos"),
        )
    )
