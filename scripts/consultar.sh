#!/usr/bin/env bash
# Corre una consulta SQL en el SQL warehouse y muestra el resultado como tabla.
# Sirve para verificar desde la terminal lo que hace el pipeline.
#
# Uso: scripts/consultar.sh "SELECT count(*) FROM oil_dev.landing..."
# El warehouse se elige con DATABRICKS_WAREHOUSE_ID o, si no está, el primero de la lista.
set -euo pipefail

# En Git Bash para Windows, sin esto la ruta /api/... se convierte en una ruta de disco.
export MSYS_NO_PATHCONV=1

SQL="${1:?falta la consulta}"
WAREHOUSE="${DATABRICKS_WAREHOUSE_ID:-$(databricks warehouses list -o json | python -c \
  "import json,sys; d=json.load(sys.stdin); d=d if isinstance(d,list) else d.get('warehouses',[]); print(d[0]['id'])")}"

CUERPO=$(python -c "import json,sys; print(json.dumps({'warehouse_id': sys.argv[1], 'statement': sys.argv[2], 'wait_timeout': '50s'}))" "$WAREHOUSE" "$SQL")

databricks api post /api/2.0/sql/statements --json "$CUERPO" | python -c "
import json, sys
r = json.load(sys.stdin)
estado = r['status']['state']
if estado != 'SUCCEEDED':
    sys.exit(f\"{estado}: {r['status'].get('error', {}).get('message', '')}\")
columnas = [c['name'] for c in r['manifest']['schema']['columns']]
filas = r.get('result', {}).get('data_array', []) or []
anchos = [max([len(c)] + [len(str(f[i])) for f in filas]) for i, c in enumerate(columnas)]
print(' | '.join(c.ljust(a) for c, a in zip(columnas, anchos)))
print('-+-'.join('-' * a for a in anchos))
for f in filas:
    print(' | '.join(str(v).ljust(a) for v, a in zip(f, anchos)))
"
