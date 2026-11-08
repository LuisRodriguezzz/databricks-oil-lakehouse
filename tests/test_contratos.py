"""Tests de los contratos (src/ingesta/utilities/contratos.py) con su SQL de verdad.

Pasan una fila completa por los mismos pasos que el pipeline: si una regla tiene un error de
sintaxis o deja pasar algo que no debe, falla acá y no en Databricks.
"""

import pytest
from pyspark.sql import functions as F

from ayuda import bronze
from utilities import contratos
from utilities.silver_comun import (
    marcar,
    sin_nulos,
    tipar,
    ultima_entrega,
    una_version_por_clave,
)

# Una declaración real y válida del pozo 157982 (enero de 2024), con todas las columnas.
VALIDA = {
    "idempresa": "YPF",
    "anio": "2024",
    "mes": "1",
    "idpozo": "157982",
    "prod_pet": "49.23",
    "prod_gas": "10.5",
    "prod_agua": "1.2",
    "iny_agua": "0",
    "iny_gas": "0",
    "iny_co2": "0",
    "iny_otro": "0",
    "tef": "31",
    "vida_util": None,
    "tipoextraccion": "Surgencia Natural",
    "tipoestado": "Extracción Efectiva",
    "tipopozo": "Petrolífero",
    "observaciones": None,
    "fechaingreso": "2024-02-16 17:45:24.72974",
    "rectificado": "f",
    "habilitado": "t",
    "idusuario": "usuario",
    "empresa": "YPF S.A.",
    "sigla": "YPF.Nq.X-1",
    "formprod": "VMUT",
    "profundidad": "3000",
    "formacion": "vaca muerta",
    "idareapermisoconcesion": "LLL",
    "areapermisoconcesion": "LOMA CAMPANA",
    "idareayacimiento": "LLL",
    "areayacimiento": "LOMA CAMPANA",
    "cuenca": "NEUQUINA",
    "provincia": "Neuquén",
    "tipo_de_recurso": "NO CONVENCIONAL",
    "proyecto": "Sin Proyecto",
    "clasificacion": "EXPLOTACION",
    "subclasificacion": "DESARROLLO",
    "sub_tipo_recurso": "SHALE",
    "fecha_data": "2024-01-31",
    "id": "157982202401",
}


def motivos_de_produccion(spark, **cambios) -> list[str]:
    """Los motivos de cuarentena de la fila válida con los cambios pedidos."""
    contrato = contratos.PRODUCCION
    reglas = {**contrato["reglas"], **contratos.REGLAS_COMUNES}
    columnas = list(contrato["tipos"])
    df = bronze(spark, [{**VALIDA, **cambios}], columnas)
    df = ultima_entrega(df, contrato["anio_en_el_archivo"])
    df = tipar(df, contrato["tipos"])
    df = una_version_por_clave(df, contrato["clave"], contrato["version"], columnas)
    return marcar(df, reglas).collect()[0]["_motivos"]


def test_el_contrato_cubre_todas_las_columnas_de_la_fila_real():
    assert set(contratos.PRODUCCION["tipos"]) == set(VALIDA)


def test_una_fila_valida_no_tiene_motivos(spark):
    assert motivos_de_produccion(spark) == []


@pytest.mark.parametrize(
    ("cambios", "motivo"),
    [
        ({"anio": "2023"}, "anio_de_la_entrega"),
        ({"prod_gas": "12,5"}, "valores_convierten"),
        ({"tef": "800"}, "tef_en_rango"),
        ({"mes": "13"}, "mes_valido"),
        ({"prod_pet": "-1"}, "produccion_no_negativa"),
        ({"empresa": None}, "empresa_informada"),
        ({"tipo_de_recurso": "OTRO"}, "tipo_de_recurso_conocido"),
        ({"_rescued_data": '{"columna_nueva": "x"}'}, "fila_bien_formada"),
    ],
)
def test_cada_regla_manda_su_caso_a_cuarentena(spark, cambios, motivo):
    assert motivo in motivos_de_produccion(spark, **cambios)


@pytest.mark.parametrize(
    ("vigentes", "cuarentena", "porcentaje", "falla"),
    [
        (100, 0, 0.0, None),
        (99, 1, 1.0, None),  # justo en el umbral: pasa
        (95, 5, 5.0, "cuarentena_bajo_umbral"),
        (0, 0, 0.0, "silver_con_filas"),
    ],
)
def test_el_control_frena_entregas_vacias_o_con_demasiada_cuarentena(
    spark, vigentes, cuarentena, porcentaje, falla
):
    df = spark.createDataFrame(
        [(vigentes, cuarentena, porcentaje, 1.0)],
        "filas_vigentes long, filas_cuarentena long, porcentaje_cuarentena double, "
        "umbral_porcentaje double",
    )

    fila = df.select(
        *[F.expr(regla).alias(nombre) for nombre, regla in sin_nulos(contratos.CONTROL).items()]
    ).collect()[0]

    fallas = [nombre for nombre, cumple in fila.asDict().items() if not cumple]
    assert fallas == ([falla] if falla else [])
