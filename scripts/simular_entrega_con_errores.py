"""Arma una entrega de producción con un porcentaje de filas mal formateadas, para probar el freno.

Toma la entrega original y, en una de cada N filas (siempre las mismas), escribe prod_gas con
coma decimal ("12,5"): un error de formato de toda la entrega, no una fila suelta.

Resultado esperado con 5 %: unas 2.298 filas en cuarentena (5 % de 45.943), por encima del
umbral del 1 %. La tarea de ingesta falla en silver.control_calidad, gold no corre y conserva
su versión anterior, y llega el mail de falla.

Uso: python scripts/simular_entrega_con_errores.py <original.csv> <destino.csv> <porcentaje>
"""

import csv
import sys
from pathlib import Path


def main(origen: str, destino: str, porcentaje: str) -> None:
    cada = round(100 / float(porcentaje))
    with open(origen, newline="", encoding="utf-8-sig") as f:
        lector = csv.DictReader(f)
        columnas = lector.fieldnames
        filas = list(lector)

    rotas = 0
    for i, fila in enumerate(filas):
        if i % cada == 0:
            fila["prod_gas"] = "12,5"
            rotas += 1

    Path(destino).parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=columnas)
        escritor.writeheader()
        escritor.writerows(filas)
    print(f"filas: {len(filas)}, con coma decimal: {rotas} ({rotas / len(filas):.2%})")


if __name__ == "__main__":
    main(*sys.argv[1:4])
