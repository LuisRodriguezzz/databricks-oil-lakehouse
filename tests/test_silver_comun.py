"""Tests de los pasos de silver (src/ingesta/utilities/silver_comun.py).

Son los mismos casos que se probaron a mano con la entrega corregida (docs/decisiones.md),
ahora en tablitas de pocas filas que corren en cada pull request.
"""

from ayuda import archivo, bronze
from utilities import contratos
from utilities.silver_comun import marcar, tipar, ultima_entrega, una_version_por_clave

ANIO = contratos.PRODUCCION["anio_en_el_archivo"]

# Un contrato chico para probar los pasos sin escribir las 39 columnas.
TIPOS = {
    "idpozo": "bigint",
    "anio": "int",
    "mes": "int",
    "prod_pet": "double",
    "prod_gas": "double",
    "fechaingreso": "timestamp",
}
CLAVE = ["idpozo", "anio", "mes"]


def fila(**valores) -> dict:
    """Una declaración válida del pozo 1 en enero de 2024; los argumentos cambian lo necesario."""
    base = {
        "idpozo": "1",
        "anio": "2024",
        "mes": "1",
        "prod_pet": "10.5",
        "prod_gas": "20.0",
        "fechaingreso": "2024-02-01 10:00:00",
    }
    return {**base, **valores}


def silver_hasta_versiones(spark, filas):
    """Bronze -> última entrega -> tipos -> una versión por clave, como en el pipeline."""
    df = ultima_entrega(bronze(spark, filas, list(TIPOS)), ANIO)
    df = tipar(df, TIPOS)
    return una_version_por_clave(df, CLAVE, "fechaingreso", list(TIPOS))


# ultima_entrega


def test_queda_la_ultima_entrega_de_cada_anio(spark):
    vieja_2024 = archivo("2026-11-07", "produccion_2024_x.csv")
    nueva_2024 = archivo("2026-11-08", "produccion_2024_x.csv")
    unica_2023 = archivo("2026-11-07", "produccion_2023_x.csv")
    filas = [
        fila(_archivo=vieja_2024),
        fila(_archivo=vieja_2024, mes="2"),
        fila(_archivo=nueva_2024),
        fila(_archivo=unica_2023, anio="2023"),
    ]

    resultado = ultima_entrega(bronze(spark, filas, list(TIPOS)), ANIO)

    archivos = {(r["_archivo"], r["_anio_entrega"]) for r in resultado.collect()}
    assert archivos == {(nueva_2024, 2024), (unica_2023, 2023)}
    assert resultado.count() == 2  # la fila de febrero solo estaba en la entrega vieja


def test_archivo_sin_anio_en_el_nombre_queda_con_anio_nulo(spark):
    filas = [fila(_archivo=archivo("2026-11-07", "datos.csv"))]

    resultado = ultima_entrega(bronze(spark, filas, list(TIPOS)), ANIO).collect()

    assert resultado[0]["_anio_entrega"] is None


def test_fuente_sin_anio_vale_la_ultima_entrega_de_todas(spark):
    filas = [
        fila(_archivo=archivo("2026-11-07", "padron.csv")),
        fila(_archivo=archivo("2026-11-08", "padron.csv"), idpozo="2"),
    ]

    resultado = ultima_entrega(bronze(spark, filas, list(TIPOS)), None).collect()

    assert [r["idpozo"] for r in resultado] == ["2"]


# tipar


def test_un_valor_que_no_convierte_queda_anotado_con_su_texto_original(spark):
    filas = [fila(prod_gas="12,5")]
    df = ultima_entrega(bronze(spark, filas, list(TIPOS)), ANIO)

    resultado = tipar(df, TIPOS).collect()[0]

    assert resultado["prod_gas"] is None
    assert resultado["_no_convierte"] == ["prod_gas"]
    assert resultado["_original"]["prod_gas"] == "12,5"


def test_un_valor_vacio_no_es_un_error_de_conversion(spark):
    filas = [fila(prod_gas=None)]
    df = ultima_entrega(bronze(spark, filas, list(TIPOS)), ANIO)

    resultado = tipar(df, TIPOS).collect()[0]

    assert resultado["prod_gas"] is None
    assert resultado["_no_convierte"] == []


def test_los_tipos_quedan_convertidos(spark):
    df = ultima_entrega(bronze(spark, [fila()], list(TIPOS)), ANIO)

    resultado = tipar(df, TIPOS)

    tipos = dict(resultado.dtypes)
    assert tipos["idpozo"] == "bigint"
    assert tipos["prod_pet"] == "double"
    assert tipos["fechaingreso"] == "timestamp"


# una_version_por_clave


def test_la_rectificativa_le_gana_a_la_declaracion_original(spark):
    filas = [
        fila(prod_pet="10.5", fechaingreso="2024-02-01 10:00:00"),
        fila(prod_pet="99.0", fechaingreso="2024-03-01 10:00:00"),
    ]

    resultado = silver_hasta_versiones(spark, filas).collect()

    assert [r["prod_pet"] for r in resultado] == [99.0]
    assert resultado[0]["_versiones"] == 1


def test_las_copias_identicas_quedan_en_una(spark):
    resultado = silver_hasta_versiones(spark, [fila(), fila()]).collect()

    assert len(resultado) == 1
    assert resultado[0]["_versiones"] == 1


def test_dos_versiones_distintas_con_la_misma_fecha_quedan_en_conflicto(spark):
    resultado = silver_hasta_versiones(spark, [fila(prod_pet="10.5"), fila(prod_pet="11.5")])

    assert sorted(r["_versiones"] for r in resultado.collect()) == [2, 2]


def test_una_fila_que_no_convierte_no_se_descarta_aunque_sea_vieja(spark):
    filas = [
        fila(fechaingreso="2024-03-01 10:00:00"),
        fila(prod_gas="12,5", fechaingreso="2024-02-01 10:00:00"),
    ]

    resultado = silver_hasta_versiones(spark, filas).collect()

    rota = [r for r in resultado if r["_no_convierte"]]
    sana = [r for r in resultado if not r["_no_convierte"]]
    assert len(rota) == 1  # llega a la cuarentena
    assert sana[0]["_versiones"] == 1  # la sana no queda en conflicto por la rota


# marcar


def test_una_regla_que_da_nulo_cuenta_como_falla(spark):
    df = spark.createDataFrame([(None,), (5.0,)], "tef double")

    resultado = marcar(df, {"tef_en_rango": "tef BETWEEN 0 AND 744"}).collect()

    assert [r["_motivos"] for r in resultado] == [["tef_en_rango"], []]
