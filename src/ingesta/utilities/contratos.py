"""Contratos de silver: tipo de cada columna, clave y reglas de calidad de cada fuente.

Viven fuera del código de las tablas, como recomienda Databricks para las reglas de calidad:
cambiar una regla no toca la lógica del pipeline, y se revisa sola en un pull request.

Cada regla es una condición SQL que una fila válida cumple. Si da falso o nulo, la fila va a
cuarentena y el nombre de la regla queda como motivo. Las columnas que empiezan con `_` las
calcula silver_comun.py antes de validar.
"""

# Reglas que valen para todas las fuentes.
REGLAS_COMUNES = {
    # Un valor que vino escrito y no se pudo convertir a su tipo (por ejemplo "12,5").
    "valores_convierten": "size(_no_convierte) = 0",
    # Auto Loader guarda en _rescued_data lo que no encajó en las columnas del archivo.
    "fila_bien_formada": "_rescued_data IS NULL",
    # Dos versiones distintas de la misma clave con la misma fecha: no se sabe cuál vale.
    "sin_versiones_en_conflicto": "_versiones <= 1",
}

# Control de cada entrega: una fila por fuente en silver.control_calidad. Si una regla no se
# cumple, falla la corrida de ingesta y el job no corre gold, que se queda con su última versión
# buena. Son expectations en modo fail: frenan, no miden.
CONTROL = {
    # Una entrega vacía dejaría silver y gold vacíos sin ningún error.
    "silver_con_filas": "filas_vigentes > 0",
    # Muchas filas malas juntas son un problema de formato de la entrega, no filas sueltas.
    "cuarentena_bajo_umbral": "porcentaje_cuarentena <= umbral_porcentaje",
}

PRODUCCION = {
    "descripcion": (
        "Producción mensual vigente por pozo: una fila por pozo, año y mes, tomada de la última "
        "entrega de cada año. Solo filas que cumplen el contrato."
    ),
    # El portal publica un archivo por año; el año de la entrega sale del nombre del archivo.
    "anio_en_el_archivo": r"produccion_(\d{4})[^/]*$",
    "clave": ["idpozo", "anio", "mes"],
    # Entre dos declaraciones de la misma clave gana la de fechaingreso más nueva.
    "version": "fechaingreso",
    # Por encima de este porcentaje de filas en cuarentena, gold no se actualiza.
    "umbral_cuarentena_pct": 1.0,
    "tipos": {
        "idempresa": "string",
        "anio": "int",
        "mes": "int",
        "idpozo": "bigint",
        "prod_pet": "double",
        "prod_gas": "double",
        "prod_agua": "double",
        "iny_agua": "double",
        "iny_gas": "double",
        "iny_co2": "double",
        "iny_otro": "double",
        "tef": "double",
        "vida_util": "double",
        "tipoextraccion": "string",
        "tipoestado": "string",
        "tipopozo": "string",
        "observaciones": "string",
        "fechaingreso": "timestamp",
        "rectificado": "boolean",
        "habilitado": "boolean",
        "idusuario": "string",
        "empresa": "string",
        "sigla": "string",
        "formprod": "string",
        "profundidad": "double",
        "formacion": "string",
        "idareapermisoconcesion": "string",
        "areapermisoconcesion": "string",
        "idareayacimiento": "string",
        "areayacimiento": "string",
        "cuenca": "string",
        "provincia": "string",
        "tipo_de_recurso": "string",
        "proyecto": "string",
        "clasificacion": "string",
        "subclasificacion": "string",
        "sub_tipo_recurso": "string",
        "fecha_data": "date",
        "id": "string",
    },
    "reglas": {
        "clave_completa": "idpozo IS NOT NULL AND anio IS NOT NULL AND mes IS NOT NULL",
        "mes_valido": "mes BETWEEN 1 AND 12",
        # Una fila de otro año dentro del archivo de 2024 no es de esta entrega.
        "anio_de_la_entrega": "anio = _anio_entrega",
        "empresa_informada": "empresa IS NOT NULL",
        # Unidad sin confirmar: ver la nota de tef en docs/decisiones.md.
        "tef_en_rango": "tef IS NULL OR tef BETWEEN 0 AND 744",
        "produccion_no_negativa": (
            "coalesce(prod_pet, 0) >= 0 AND coalesce(prod_gas, 0) >= 0 "
            "AND coalesce(prod_agua, 0) >= 0"
        ),
        "inyeccion_no_negativa": (
            "coalesce(iny_agua, 0) >= 0 AND coalesce(iny_gas, 0) >= 0 "
            "AND coalesce(iny_co2, 0) >= 0 AND coalesce(iny_otro, 0) >= 0"
        ),
        "profundidad_y_vida_util_no_negativas": (
            "coalesce(profundidad, 0) >= 0 AND coalesce(vida_util, 0) >= 0"
        ),
        "tipo_de_recurso_conocido": (
            "tipo_de_recurso IS NULL OR tipo_de_recurso IN "
            "('CONVENCIONAL', 'NO CONVENCIONAL', 'SIN RESERVORIO', 'NO DISCRIMINADO')"
        ),
    },
}

PADRON = {
    "descripcion": (
        "Padrón vigente de pozos: una fila por pozo con el año y el mes de su primera "
        "producción, tomada de la última entrega. Solo filas que cumplen el contrato."
    ),
    # El padrón llega entero en cada entrega, sin año: vale la última.
    "anio_en_el_archivo": None,
    "clave": ["idpozo"],
    # El archivo no trae fecha de carga por fila: dos filas distintas del mismo pozo son
    # un conflicto.
    "version": None,
    "umbral_cuarentena_pct": 1.0,
    "tipos": {
        "idpozo": "bigint",
        "anio": "int",
        "mes": "int",
    },
    "reglas": {
        "clave_completa": "idpozo IS NOT NULL",
        "primera_produccion_completa": "anio IS NOT NULL AND mes IS NOT NULL",
        "mes_valido": "mes BETWEEN 1 AND 12",
    },
}
