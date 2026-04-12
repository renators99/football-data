# Football Data Scraper

Este repositorio contiene un pipeline en PySpark para descargar los CSV de
[football-data.co.uk](https://www.football-data.co.uk), guardarlos en la capa
bronze y construir capas `silver` y `gold` con datos normalizados y agregados.

## Arquitectura del código

El código está organizado por capas dentro del paquete `football_data`:

- `football_data.bronze`: descarga los CSV y puede subirlos a Google Cloud
  Storage.
- `football_data.silver`: transforma los CSV raw en una tabla normalizada de
  partidos.
- `football_data.gold`: genera agregados curados por equipo, liga y temporada.
- `football_data.utils`: helpers de configuración, temporadas y rutas.
- `football_data_scraper.run_scraper`: entrypoint que ejecuta el pipeline
  `Bronze -> Silver -> Gold`.

Funciones principales:

- `football_data.utils.config.load_config_from_env`
- `football_data.utils.seasons.build_season_list`
- `football_data.bronze.run_spark_job`
- `football_data.silver.run_silver_layer`
- `football_data.gold.run_gold_layer`

## Estructura del proyecto

```text
football-data-backend/
  football_data/
    bronze/
    gold/
      __init__.py
    silver/
      __init__.py
    utils/
    spark_job.py
    uploader.py
  football_data_scraper.py
  README.md
  requirements.txt
```

## Estructura de salida

```text
data/
  bronze/
    football-data/
      england_premier_league/
        9394.csv
        ...
  silver/
    matches/
      league_code=E0/
        season=2324/
          part-*.parquet
  gold/
    team_season_table/
      league_code=E0/
        season=2324/
          part-*.parquet
    league_season_summary/
      league_code=E0/
        season=2324/
          part-*.parquet
```

Por defecto, la descarga raw se guarda en
`data/bronze/football-data/<liga>/<temporada>.csv`. Puedes cambiar la raíz con
`FOOTBALL_DATA_OUTPUT_DIR`.

## Variables de configuración

- `FOOTBALL_DATA_OUTPUT_DIR`: carpeta base de salida. Por defecto:
  `data/bronze/football-data`.
- `FOOTBALL_DATA_START_YEAR`: primer año histórico a descargar. Por defecto:
  `1993`.
- `FOOTBALL_DATA_PARTITIONS`: número de particiones Spark. Por defecto: `24`.
- `FOOTBALL_DATA_GCS_BUCKET`: bucket de Cloud Storage para copiar los archivos.
- `FOOTBALL_DATA_GCS_PREFIX`: prefijo dentro del bucket. Por defecto:
  `lakehouse/football-data`.
- `FOOTBALL_DATA_LEAGUE_CODES`: ligas personalizadas en formato
  `CODIGO=nombre_largo,CODIGO2=otra_liga`.

## Requisitos

- Python 3.10+
- PySpark 3.5
- Java Runtime
- Credenciales de Google Cloud si se usará subida a GCS

## Instalación rápida

```bash
python -m venv .venv
source .venv/bin/activate  # En Windows usa .venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecución local

```bash
spark-submit football_data_scraper.py
```

El pipeline ejecuta en orden:

1. Descarga de CSV a bronze.
2. Construcción de `silver/matches`.
3. Construcción de `gold/team_season_table`.
4. Construcción de `gold/league_season_summary`.

## Verificación rápida

```bash
python -m compileall football_data_scraper.py football_data/
```

Luego revisa que existan archivos en:

- `data/bronze/football-data`
- `data/silver/matches`
- `data/gold/team_season_table`
- `data/gold/league_season_summary`

## Carga automática a GCS

Si defines `FOOTBALL_DATA_GCS_BUCKET`, cada descarga exitosa se sube a Cloud
Storage respetando la misma estructura de carpetas. Ejemplo:

```bash
export FOOTBALL_DATA_GCS_BUCKET="mi-bucket-lakehouse"
export FOOTBALL_DATA_GCS_PREFIX="lakehouse/football-data"
spark-submit football_data_scraper.py
```

## Programación diaria

Puedes programar `football_data_scraper.py` en Dataproc Serverless u otro
orquestador similar para ejecutar el pipeline completo todos los días. Cada
ejecución sobrescribe las capas generadas, así que el proceso es reproducible.
