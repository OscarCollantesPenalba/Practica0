# Práctica 0 - IBEX35 con PySpark y MySQL

## Requisitos
- Entorno conda `mineriadedatos2026` (Python 3.10, PySpark 3.3, OpenJDK 17)
- MySQL en local y el conector JDBC en `lib/`

## Preparación
1. Activar el entorno: `conda activate mineriadedatos2026`
2. Crear la base de datos (una sola vez): `CREATE DATABASE IBEX35;`
3. Definir las credenciales de MySQL como variables de entorno (si no se definen, se usa `root` sin contraseña):
   - Windows (CMD): `setx DB_USER "usuario"` y `setx DB_PASSWORD "clave"` (reabrir la terminal)
   - Linux/macOS: `export DB_USER=usuario` y `export DB_PASSWORD=clave`

## Ejecución
Desde la raíz del proyecto: `python main.py`

## Base de datos
Al final se guardan los datos en la tabla `Datos2024` de la base `IBEX35`: primero los datos completos del CSV y después los datos tratados (sin las columnas nuevas de Ej3, Ej5 y Ej6), que sustituyen a los anteriores. Si MySQL no está disponible, el programa lo indica por consola y el resto de ejercicios se ejecuta igualmente.

## Estructura (MVC)
- `src/modelo/`: sesión de Spark, conexión JDBC, carga y transformación de datos
- `src/vista/`: salida por consola
- `src/controlador/`: orquesta los ejercicios
- `data/`: CSV · `lib/`: conector MySQL