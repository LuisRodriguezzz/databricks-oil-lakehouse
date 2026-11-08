"""Arma una entrega corregida de producción con seis casos conocidos, para probar silver.

Toma la entrega original y le aplica, cada caso sobre un pozo distinto:

1. corregido:     cambia prod_pet de una fila (el portal corrigió un valor).
2. borrado:       saca una fila (el portal la retiró).
3. no_convierte:  escribe prod_gas con coma decimal ("12,5").
4. otro_anio:     pone anio = 2023 dentro del archivo de 2024.
5. copia_exacta:  repite una fila tal cual.
6. en_conflicto:  repite una fila con otro prod_pet y la misma fechaingreso.

Resultado esperado en silver después de procesarla:
- produccion: 45.943 - 4 = 45.939 filas (borrado, no_convierte, otro_anio y en_conflicto
  salen; la copia exacta queda en una) y el valor corregido es el nuevo.
- produccion_cuarentena: 4 filas (no_convierte, otro_anio y las dos versiones en conflicto).

Uso: python scripts/simular_correccion.py <entrega_original.csv> <entrega_corregida.csv>
Imprime el idpozo de cada caso, para buscarlo después en silver.
"""

import csv
import sys
from pathlib import Path


def main(origen: str, destino: str) -> None:
    with open(origen, newline="", encoding="utf-8-sig") as f:
        lector = csv.DictReader(f)
        columnas = lector.fieldnames
        filas = list(lector)

    # Seis filas de pozos distintos, elegidas siempre igual (las primeras del archivo).
    elegidas, pozos = [], set()
    for i, fila in enumerate(filas):
        if fila["idpozo"] not in pozos:
            elegidas.append(i)
            pozos.add(fila["idpozo"])
        if len(elegidas) == 6:
            break
    corregido, borrado, no_convierte, otro_anio, copia_exacta, en_conflicto = elegidas

    casos = {
        "corregido": filas[corregido]["idpozo"],
        "borrado": filas[borrado]["idpozo"],
        "no_convierte": filas[no_convierte]["idpozo"],
        "otro_anio": filas[otro_anio]["idpozo"],
        "copia_exacta": filas[copia_exacta]["idpozo"],
        "en_conflicto": filas[en_conflicto]["idpozo"],
    }

    filas[corregido]["prod_pet"] = "999.5"
    filas[no_convierte]["prod_gas"] = "12,5"
    filas[otro_anio]["anio"] = "2023"
    filas.append(dict(filas[copia_exacta]))
    conflicto = dict(filas[en_conflicto])
    conflicto["prod_pet"] = str(float(conflicto["prod_pet"] or 0) + 1)
    filas.append(conflicto)
    del filas[borrado]

    Path(destino).parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=columnas)
        escritor.writeheader()
        escritor.writerows(filas)

    for caso, idpozo in casos.items():
        print(f"{caso}: idpozo {idpozo}")
    print(f"filas escritas: {len(filas)}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
