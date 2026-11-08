"""Pasos de silver que son iguales para todas las fuentes.

Cada función recibe y devuelve un DataFrame; ninguna escribe nada. El orden lo arma
transformations/silver.py:

    última entrega -> tipos -> una versión por clave -> motivos de cuarentena
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Columnas de bronze que silver conserva para rastrear cada fila hasta su archivo.
LINAJE = ["ingest_date", "_archivo", "_archivo_modificado_en", "_cargado_en"]


def ultima_entrega(bronze: DataFrame, anio_en_el_archivo: str | None) -> DataFrame:
    """Solo las filas de la última entrega de cada año.

    Cada entrega es una foto completa de su año: la última manda, así que una fila corregida
    toma el valor nuevo y una fila que el portal sacó deja de estar en silver. Si la fuente no
    se entrega por año (`anio_en_el_archivo` es None), vale la última entrega de todas.

    El año sale del nombre del archivo y queda en `_anio_entrega`; si el nombre no lo trae,
    queda nulo y la regla `anio_de_la_entrega` manda esas filas a cuarentena.
    """
    if anio_en_el_archivo:
        anio = F.regexp_extract("_archivo", anio_en_el_archivo, 1)
        bronze = bronze.withColumn("_anio_entrega", F.when(anio != "", anio.cast("int")))
    else:
        bronze = bronze.withColumn("_anio_entrega", F.lit(None).cast("int"))

    # Una entrega es un archivo. Gana la carpeta de fecha más nueva; si dos archivos del mismo
    # año llegaran el mismo día, desempata el nombre, para que el resultado sea siempre igual.
    entregas = bronze.select("_anio_entrega", "ingest_date", "_archivo").distinct()
    orden = Window.partitionBy("_anio_entrega").orderBy(F.desc("ingest_date"), F.desc("_archivo"))
    vigentes = (
        entregas.withColumn("_n", F.row_number().over(orden)).filter("_n = 1").select("_archivo")
    )
    return bronze.join(vigentes, "_archivo", "left_semi")


def tipar(filas: DataFrame, tipos: dict[str, str]) -> DataFrame:
    """Cada columna a su tipo, sin perder lo que no convierte.

    `try_cast` devuelve nulo en vez de fallar. Para que ese nulo no pase callado, si el texto
    original no estaba vacío y no convirtió, el nombre de la columna se anota en
    `_no_convierte`. El texto original de toda la fila queda en `_original`, para la cuarentena.
    """
    convertidas = {
        columna: F.expr(f"try_cast(`{columna}` AS {tipo})")
        for columna, tipo in tipos.items()
        if tipo != "string"
    }
    tipadas = [convertidas.get(columna, F.col(columna)).alias(columna) for columna in tipos]
    no_convierte = F.filter(
        F.array(
            *[
                F.when(F.col(columna).isNotNull() & valor.isNull(), F.lit(columna))
                for columna, valor in convertidas.items()
            ]
        ),
        lambda nombre: nombre.isNotNull(),
    )
    original = F.struct(*[F.col(columna) for columna in tipos])
    return filas.select(
        *tipadas,
        no_convierte.alias("_no_convierte"),
        original.alias("_original"),
        "_anio_entrega",
        "_rescued_data",
        *LINAJE,
    )


def una_version_por_clave(
    filas: DataFrame, clave: list[str], version: str | None, columnas: list[str]
) -> DataFrame:
    """Deja una sola fila por clave, sin elegir al azar.

    1. Si la fuente tiene columna de versión, se queda con la más nueva de cada clave (una
       rectificativa reemplaza a la declaración original). Una fila con un valor que no
       convirtió nunca se descarta acá: tiene que llegar a la cuarentena.
    2. Las copias idénticas quedan en una.
    3. Si todavía quedan dos versiones distintas, se cuentan en `_versiones` y la regla
       `sin_versiones_en_conflicto` las manda a cuarentena: no hay forma de saber cuál vale.
    """
    por_clave = Window.partitionBy(*clave)
    convierte = F.size("_no_convierte") == 0
    if version:
        # Spark no deja filtrar por una función de ventana: primero se calcula como columna.
        mas_nueva = F.col(version).eqNullSafe(F.max(version).over(por_clave))
        filas = (
            filas.withColumn("_mas_nueva", mas_nueva)
            .filter(F.col("_mas_nueva") | ~convierte)
            .drop("_mas_nueva")
        )
    filas = filas.dropDuplicates(columnas)
    return filas.withColumn("_versiones", F.count(F.when(convierte, 1)).over(por_clave))


def sin_nulos(reglas: dict[str, str]) -> dict[str, str]:
    """Cada regla con nulo = falla. En SQL, `NULL BETWEEN 0 AND 744` no es ni verdadero ni falso."""
    return {nombre: f"coalesce(({condicion}), false)" for nombre, condicion in reglas.items()}


def marcar(filas: DataFrame, reglas: dict[str, str]) -> DataFrame:
    """Agrega `_motivos`: los nombres de las reglas que la fila no cumple (vacío si está bien)."""
    fallas = [
        F.when(~F.expr(condicion), F.lit(nombre)) for nombre, condicion in sin_nulos(reglas).items()
    ]
    return filas.withColumn(
        "_motivos", F.filter(F.array(*fallas), lambda nombre: nombre.isNotNull())
    )
