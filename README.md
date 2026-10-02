# Práctica 0 - IBEX35 con PySpark y MySQL

Repositorio en GitHub: https://github.com/OscarCollantesPenalba/Practica0

Análisis de los precios de cierre de las empresas del IBEX 35 en 2024 con PySpark
(carga, limpieza, estadísticas, cuartiles y variaciones diarias) y almacenamiento
del resultado en una base de datos MySQL mediante JDBC.

## Requisitos
- Entorno conda `mineriadedatos2026` (Python 3.10, PySpark 3.3, OpenJDK 17)
- MySQL en local (el conector JDBC ya está incluido en `lib/`)

## Preparación
1. Activar el entorno: `conda activate mineriadedatos2026`
2. Crear la base de datos (una sola vez): `CREATE DATABASE IBEX35;`
3. Definir las credenciales de MySQL como variables de entorno

## Ejecución
Desde la raíz del proyecto: `python main.py`

Cada ejercicio imprime su identificador (`Ej1-a`, `Ej1-b`, ..., `Ej6`) seguido de lo
solicitado, y al final aparece el bloque `BD` con el guardado en MySQL.

## Base de datos
Al final se guardan los datos en la tabla `Datos2024` de la base `IBEX35`: primero los
datos completos del CSV y después los datos tratados (sin las columnas nuevas de Ej3,
Ej5 y Ej6), que sustituyen a los anteriores. Si MySQL no está disponible, el programa
lo indica por consola y el resto de ejercicios se ejecuta igualmente.

## Estructura (MVC)
- `main.py`: punto de entrada
- `src/modelo/`: sesión de Spark (`spark_session.py`), conexión JDBC (`db_conexion.py`),
  carga de datos (`data_loader.py`, `ibex_schema.py`) y transformaciones (`ibex_model.py`)
- `src/vista/`: salida por consola
- `src/controlador/`: orquesta los ejercicios
- `data/`: CSV con los precios · `lib/`: conector MySQL (`.jar`)