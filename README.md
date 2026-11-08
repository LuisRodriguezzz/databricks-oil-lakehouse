# Oil lakehouse en Databricks

Laboratorio: el lakehouse de petróleo y gas de la Secretaría de Energía, construido con las
prácticas actuales de Databricks (Unity Catalog, pipelines declarativos, Lakeflow Jobs y
Declarative Automation Bundles). Datos reales: producción 2024 de la cuenca Neuquina no
convencional y el padrón de pozos con primera producción.

## Estructura

| Ruta | Qué es |
| --- | --- |
| `databricks.yml` | El bundle: qué se despliega y en qué ambiente |
| `resources/` | Recursos del bundle: esquemas por capa (después, pipelines y jobs) |
| `scripts/plataforma.sh` | Crea el catálogo y landing; lo de plataforma, una vez por ambiente |
| `scripts/subir_a_landing.sh` | Sube un archivo a landing sin pisar nada |
| `src/ingesta/transformations/` | Tablas del pipeline declarativo: `bronze.py` y `silver.py` |
| `src/ingesta/utilities/` | Módulos que importa el pipeline: contratos (tipos y reglas) y pasos de silver |
| `src/gold/transformations/` | Tablas de gold, en su propio pipeline |
| `resources/job_oil.yml` | Job que corre la ingesta y, si pasó el control de calidad, gold |
| `scripts/simular_entrega_con_errores.py` | Arma una entrega con un % de filas mal formateadas, para probar el freno |
| `scripts/consultar.sh` | Corre una consulta SQL en el warehouse, para verificar |
| `scripts/simular_correccion.py` | Arma una entrega corregida con casos conocidos, para probar silver |
| `docs/decisiones.md` | Cada decisión con su porqué y la alternativa descartada |
| `tests/` | Tests de la lógica de silver y de los contratos, con PySpark local |
| `.github/workflows/` | CI en cada pull request; CD a prod al hacer merge a main |
| `ci/azure-pipelines.yml` | El mismo CI/CD escrito para Azure DevOps (referencia, no se corrió) |
| `pyproject.toml`, `requirements-dev.txt` | Configuración y versiones de pytest, ruff y PySpark |
| `datos/` | Muestras locales; no se versionan |

## Cómo se usa

En Free Edition el catálogo `oil_<ambiente>` se crea desde la interfaz (Catalog > + > Create
catalog): con Default Storage, la API no lo crea. El resto lo hacen los scripts y el bundle.

`DATABRICKS_BUNDLE_ENGINE=direct` despliega sin Terraform. Hace falta con la CLI v0.283.0: su
motor Terraform falla al verificar la firma de la descarga (clave vencida).

```bash
export DATABRICKS_CONFIG_PROFILE=oil-lab DATABRICKS_BUNDLE_ENGINE=direct
scripts/plataforma.sh dev
scripts/subir_a_landing.sh dev produccion datos/produccion_2024_neuquina_no_convencional.csv 2026-11-07
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev oil_lakehouse   # en dev el disparador está pausado
```

Si la corrida falla por el control de calidad, se corrige la entrega (un archivo nuevo en otra
fecha) y se repara la misma corrida: `databricks jobs repair-run <run_id> --rerun-all-failed-tasks`.

## Cómo se trabaja en equipo

1. Cada uno despliega su copia de dev desde su PC (`databricks bundle deploy -t dev`).
2. Los cambios van en una rama y se abren como pull request a `main`.
3. El CI (`.github/workflows/ci.yml`) corre los tests, `ruff` y la validación de los dos targets.
4. Con el CI en verde y la revisión hecha, se hace merge.
5. El CD (`.github/workflows/cd.yml`) vuelve a probar lo que quedó en `main` y despliega prod
   como el service principal `oil-lakehouse-prod`. El job de prod corre cuando llega una entrega.

Los tests corren con PySpark local y necesitan Java 17 o superior:

```bash
pip install -r requirements-dev.txt
ruff check .
pytest
```

## Preparar prod (una vez)

1. Crear el catálogo `oil_prod` desde la interfaz.
2. Crear el service principal `oil-lakehouse-prod` (Settings > Identity and access > Service
   principals), con *Workspace access*, y generarle un OAuth secret.
3. `scripts/plataforma.sh prod <application-id>`: crea landing en `oil_prod` y le da al service
   principal los permisos mínimos.
4. En el repo de GitHub: la variable `DATABRICKS_HOST` y los secretos `DATABRICKS_CLIENT_ID` y
   `DATABRICKS_CLIENT_SECRET`, este último cargado por quien lo generó.
