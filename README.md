# Football Data Scraper (PySpark + Lakehouse)

Pipeline en Python/PySpark para descargar CSV de `football-data.co.uk` y construir capas **bronze, silver y gold** listas para data lake en local y en GCP.

## Ligas incluidas

- Inglaterra: `E0` (Premier League), `E1` (Championship)
- España: `SP1` (La Liga), `SP2` (Segunda)
- Italia: `I1`, `I2`
- Francia: `F1`, `F2`
- Alemania: `D1`, `D2`
- Holanda: `N1`
- Portugal: `P1`

## Estructura del proyecto (más dividida por librerías)

- `football_data/config.py`: variables de entorno y configuración.
- `football_data/seasons.py`: generación de temporadas (`9394`, `2425`, etc.).
- `football_data/downloader.py`: armado de tareas y descarga de CSV (bronze).
- `football_data/silver.py`: normalización a esquema de partidos.
- `football_data/gold.py`: agregados de negocio (tabla por equipo y resumen de liga).
- `football_data/layers.py`: rutas de capas del lakehouse.
- `football_data/uploader.py`: subida del lakehouse completo a GCS.
- `football_data/spark_job.py`: orquestación completa Spark.
- `football_data_scraper.py`: entrypoint.

## Capas Lakehouse

Por defecto se escribe en `data/lakehouse`:

```text
data/lakehouse/
  bronze/
    football-data/
      league_code=E0/league_name=england_premier_league/season=2324/data.csv
  silver/
    matches/
      league_code=E0/season=2324/part-*.parquet
  gold/
    team_season_table/
      league_code=E0/season=2324/part-*.parquet
    league_season_summary/
      league_code=E0/season=2324/part-*.parquet
```

## Variables de entorno

- `FOOTBALL_DATA_OUTPUT_DIR` (default: `data/lakehouse`)
- `FOOTBALL_DATA_START_YEAR` (default: `1993`)
- `FOOTBALL_DATA_PARTITIONS` (default: `24`)
- `FOOTBALL_DATA_GCS_BUCKET` (opcional)
- `FOOTBALL_DATA_GCS_PREFIX` (default: `lakehouse`)
- `FOOTBALL_DATA_LEAGUE_CODES` (opcional `CODIGO=nombre,CODIGO2=nombre2`)

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecución

```bash
spark-submit football_data_scraper.py
```

## Carga a GCP (lakehouse)

```bash
export FOOTBALL_DATA_GCS_BUCKET="mi-bucket"
export FOOTBALL_DATA_GCS_PREFIX="lakehouse"
spark-submit football_data_scraper.py
```

Con eso se suben archivos de bronze/silver/gold al bucket.

## Prueba rápida

```bash
python -m compileall football_data_scraper.py football_data/
```

## Programación diaria 5 AM

Ejemplo crontab (UTC):

```bash
0 5 * * * /ruta/a/spark-submit /ruta/proyecto/football_data_scraper.py
```

En GCP puedes usar Cloud Scheduler + Dataproc Serverless con la misma idea.
