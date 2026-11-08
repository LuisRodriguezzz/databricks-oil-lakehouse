#!/usr/bin/env bash
# Sube un archivo a landing, en la carpeta de su fuente y de su fecha de entrega.
#
# Landing es inmutable: si el archivo ya existe en esa entrega, no se pisa y el script falla.
# Una corrección de la fuente llega como una entrega nueva, con otra fecha. Así queda la historia
# de qué llegó y cuándo, y Auto Loader procesa cada archivo una sola vez.
#
# Uso: scripts/subir_a_landing.sh <ambiente> <fuente> <archivo> [AAAA-MM-DD]
set -euo pipefail

AMBIENTE="${1:?falta el ambiente}"
FUENTE="${2:?falta la fuente}"
ARCHIVO="${3:?falta el archivo}"
FECHA="${4:-$(date +%F)}"

CARPETA="dbfs:/Volumes/oil_${AMBIENTE}/landing/archivos/${FUENTE}/ingest_date=${FECHA}"
DESTINO="${CARPETA}/$(basename "$ARCHIVO")"

if databricks fs ls "$CARPETA" 2>/dev/null | grep -qxF "$(basename "$ARCHIVO")"; then
  echo "ya existe $DESTINO: landing no se pisa; usá otra fecha de entrega" >&2
  exit 1
fi

databricks fs mkdir "$CARPETA"
databricks fs cp "$ARCHIVO" "$DESTINO"
echo "subido: $DESTINO"
