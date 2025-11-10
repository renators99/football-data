# Football Data Scraper

Este repositorio contiene un conjunto de componentes modulares en PySpark que
automatizan la descarga diaria de los CSV publicados por
[football-data.co.uk](https://www.football-data.co.uk) para las ligas inglesa (1.ª y
2.ª división), española, italiana, francesa, alemana, holandesa y portuguesa. Cada
ciclo recrea por completo los archivos de todas las temporadas disponibles, por lo
que puedes programarlo sin preocuparte por acumulaciones de ejecuciones previas.

## Arquitectura del código

El scraper está dividido en clases reutilizables que facilitan su mantenimiento y
extensión:

- `football_data.config.ScraperConfig`: centraliza la lectura de variables de
  entorno y resuelve rutas por liga.
- `football_data.seasons.SeasonCodeGenerator`: genera los códigos de temporada
  (`9394`, `2324`, etc.) a partir de un año inicial.
- `football_data.downloader.FootballDataDownloader`: construye tareas de descarga
  y gestiona la obtención de cada CSV.
- `football_data.uploader.GCSUploader`: copia los archivos descargados en un
  bucket de Google Cloud Storage cuando está configurado.
- `football_data.spark_job.FootballDataSparkJob`: coordina la ejecución en Spark
  para paralelizar las descargas y, opcionalmente, la subida al lakehouse.
- `football_data_scraper.run_scraper`: punto de entrada que ensambla los
  componentes anteriores para ejecuciones locales o serverless.

## Estructura de directorios de salida

```
data/
  raw/
    football-data/
      england_premier_league/
        9394.csv
        ...
      spain_la_liga/
        9394.csv
        ...
```

Los archivos se guardan en `data/raw/football-data/<liga>/<temporada>.csv` por
defecto. Puedes cambiar la ruta con la variable de entorno `FOOTBALL_DATA_OUTPUT_DIR`.

## Variables de configuración

- `FOOTBALL_DATA_OUTPUT_DIR`: carpeta de destino para los CSV (por defecto
  `data/raw/football-data`).
- `FOOTBALL_DATA_START_YEAR`: primer año de la serie histórica (por defecto
  `1993`).
- `FOOTBALL_DATA_PARTITIONS`: número de particiones Spark para paralelizar las
  descargas (por defecto `24`).
- `FOOTBALL_DATA_GCS_BUCKET`: nombre del bucket de Cloud Storage donde se
  copiarán los CSV (opcional).
- `FOOTBALL_DATA_GCS_PREFIX`: prefijo dentro del bucket (por defecto
  `lakehouse/football-data`).
- `FOOTBALL_DATA_LEAGUE_CODES`: lista personalizada de ligas en formato
  `CODIGO=nombre_largo,CODIGO2=otra_liga`. Si no se define, se usan las ligas por
  defecto mencionadas arriba.

## Requisitos

- Python 3.10+
- PySpark 3.5 (se instala automáticamente desde `requirements.txt`)
- Java Runtime (requerido por PySpark)
- SDK de Google Cloud configurado con Application Default Credentials (solo si
  deseas subir a GCS)

## Instalación rápida

```bash
python -m venv .venv
source .venv/bin/activate  # En Windows usa .venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecución local

```bash
# Dentro del entorno virtual
spark-submit football_data_scraper.py
```

Puedes pasar variables de entorno antes del comando para personalizar el
comportamiento. El job ensamblará las clases descritas en la sección de
arquitectura y ejecutará la descarga completa.

## Carga automática al Lakehouse en GCP

Cuando la variable `FOOTBALL_DATA_GCS_BUCKET` está configurada, cada descarga
exitosa se sube automáticamente a Cloud Storage siguiendo la misma estructura de
carpetas. Pasos sugeridos:

1. **Crear el bucket (una sola vez)**:

   ```bash
   gcloud storage buckets create gs://mi-bucket-lakehouse --location=EU
   ```

2. **Configurar credenciales**: asegúrate de que el job tenga credenciales con
   permisos `roles/storage.objectAdmin` sobre el bucket. En local puedes usar
   Application Default Credentials:

   ```bash
   gcloud auth application-default login
   ```

3. **Definir variables de entorno antes de ejecutar**:

   ```bash
   export FOOTBALL_DATA_GCS_BUCKET="mi-bucket-lakehouse"
   export FOOTBALL_DATA_GCS_PREFIX="lakehouse/football-data"
   spark-submit football_data_scraper.py
   ```

4. **Verificar en Cloud Storage**:

   ```bash
   gcloud storage ls gs://mi-bucket-lakehouse/lakehouse/football-data/spain_la_liga/
   ```

Estos archivos pueden conectarse a BigQuery mediante BigLake o a cualquier otro
motor de lakehouse compatible con Cloud Storage.

## ¿Cómo probar todo?

1. **Verificar dependencias**: ejecuta `python -m compileall football_data_scraper.py football_data/`
   para asegurarte de que no existan errores de sintaxis.
2. **Ejecutar el job**: corre `spark-submit football_data_scraper.py` (o
   `spark-submit --conf ...` si necesitas ajustar parámetros). El job descargará
   todas las temporadas disponibles y mostrará en consola un resumen de
   descargas exitosas.
3. **Comprobar los resultados**: revisa el directorio `data/raw/football-data`
   (o el que hayas configurado) y confirma que existan subcarpetas por liga con
   los CSV de cada temporada. Por ejemplo:

   ```bash
   ls data/raw/football-data/spain_la_liga | head
   ```

4. **Repetir en modo limpio** (opcional): elimina la carpeta de salida y vuelve
   a ejecutar el script para confirmar que la ingesta es reproducible y
   sobrescribe los datos cada vez.

## Programación diaria a las 5 AM

1. Sube el contenido de este repositorio a un bucket de GCS.
2. Crea un job serverless de PySpark (por ejemplo, Dataproc Serverless) cuyo
   entrypoint sea `football_data_scraper.py` (la función `spark_main` también
   está disponible si el servicio la requiere).
3. Asigna a la cuenta de servicio permisos `roles/storage.objectAdmin` sobre el
   bucket donde se almacenarán los datos.
4. Programa la ejecución diaria a las 05:00 con Cloud Scheduler u orquestador
   equivalente pasando las variables de entorno anteriores.

Cada ejecución sobrescribe los archivos existentes, manteniendo el dataset
actualizado sin pasos adicionales.
