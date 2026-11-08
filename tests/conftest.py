"""Una sesión de Spark local para todos los tests.

Corre en la máquina del CI, sin Databricks: prueba la lógica de silver con tablitas de pocas
filas, en segundos. ANSI activo y zona horaria UTC, como en serverless.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    sesion = (
        SparkSession.builder.master("local[1]")
        .appName("tests-oil-lakehouse")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()
