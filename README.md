# Práctica 0 - IBEX35 con PySpark y MySQL

## Requisitos
- Entorno conda `mineriadedatos2026` (Python 3.10, PySpark 3.3, OpenJDK 17)
- MySQL en local y el conector JDBC en `lib/`

## Preparación
1. Activar el entorno: `conda activate mineriadedatos2026`
2. Crear la base de datos (una sola vez): `CREATE DATABASE IBEX35;`
3. Definir las credenciales como variables de entorno:
   - Windows (CMD): `setx DB_USER "usuario"` y `setx DB_PASSWORD "clave"` (reabrir la terminal)

## Ejecución
Desde la raíz del proyecto: `python main.py`

## Estructura (MVC)
- `src/modelo/`: sesión de Spark, conexión JDBC, carga y transformación de datos
- `src/vista/`: salida por consola
- `src/controlador/`: orquesta los ejercicios
- `data/`: CSV · `lib/`: conector MySQL
