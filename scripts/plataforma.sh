#!/usr/bin/env bash
# Lo que en una empresa crea el equipo de plataforma, una vez por ambiente: el catálogo y la zona
# de llegada de archivos (landing). Va aparte del bundle porque lo comparten todos los
# desarrolladores y los pipelines solo lo leen.
#
# Es idempotente: si algo ya existe, no lo toca. Correrlo dos veces da el mismo resultado.
#
# Uso: scripts/plataforma.sh dev
#      scripts/plataforma.sh prod <application-id-del-service-principal>
#
# Con un service principal, además le da los permisos mínimos para que el bundle de prod corra
# como él: entrar al catálogo y crear sus esquemas (bronze, silver, gold), y leer landing.
# Dar un permiso que ya tiene no cambia nada, así que también es idempotente.
set -euo pipefail

AMBIENTE="${1:?Uso: scripts/plataforma.sh <ambiente> [application-id-del-service-principal]}"
SP="${2:-}"
CATALOGO="oil_${AMBIENTE}"

# El catálogo no se crea acá. En Free Edition la cuenta usa Default Storage, y la API exige una
# ubicación de almacenamiento que no existe: Databricks indica crearlo desde la interfaz
# (Catalog > + > Create catalog). En una empresa lo crea plataforma, por ejemplo con Terraform.
if databricks catalogs get "$CATALOGO" >/dev/null 2>&1; then
  echo "catálogo $CATALOGO: existe"
else
  echo "falta el catálogo $CATALOGO: crealo desde Catalog > + > Create catalog y volvé a correr" >&2
  exit 1
fi

if databricks schemas get "$CATALOGO.landing" >/dev/null 2>&1; then
  echo "esquema $CATALOGO.landing: ya existía"
else
  databricks schemas create landing "$CATALOGO" \
    --comment "Zona de llegada: archivos originales, nunca se pisan" >/dev/null
  echo "esquema $CATALOGO.landing: creado"
fi

if databricks volumes read "$CATALOGO.landing.archivos" >/dev/null 2>&1; then
  echo "volume $CATALOGO.landing.archivos: ya existía"
else
  databricks volumes create "$CATALOGO" landing archivos MANAGED \
    --comment "Una carpeta por fuente y una subcarpeta por fecha de entrega" >/dev/null
  echo "volume $CATALOGO.landing.archivos: creado"
fi

if [ -n "$SP" ]; then
  dar() {  # dar <tipo> <objeto> <permiso> [<permiso> ...]
    local tipo="$1" objeto="$2"
    shift 2
    local lista
    lista=$(printf '"%s",' "$@")
    databricks grants update "$tipo" "$objeto" \
      --json "{\"changes\": [{\"principal\": \"$SP\", \"add\": [${lista%,}]}]}" >/dev/null
    echo "permisos de $SP en $objeto: $*"
  }
  dar catalog "$CATALOGO" USE_CATALOG CREATE_SCHEMA
  dar schema "$CATALOGO.landing" USE_SCHEMA
  dar volume "$CATALOGO.landing.archivos" READ_VOLUME
fi
