# Football Data Scraper

Este repositorio contiene un único job de PySpark que descarga diariamente los CSV
de [football-data.co.uk](https://www.football-data.co.uk) para las ligas inglesa (1.ª y
2.ª división), española, italiana, francesa, alemana, holandesa y portuguesa. Cada
ejecución recrea los archivos para todas las temporadas disponibles, por lo que se
puede programar sin preocuparse por acumulaciones de datos antiguos.

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

## Requisitos

- Python 3.10+
- PySpark 3.5 (se instala automáticamente desde `requirements.txt`)
- Java Runtime (requerido por PySpark)

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

Variables opcionales:

- `FOOTBALL_DATA_OUTPUT_DIR`: carpeta de destino para los CSV (por defecto `data/raw/football-data`).
- `FOOTBALL_DATA_START_YEAR`: primer año de la serie histórica (por defecto `1993`).
- `FOOTBALL_DATA_PARTITIONS`: número de particiones Spark para paralelizar las descargas.

## Programación diaria a las 5 AM

1. Sube el contenido de este repositorio a un bucket de GCS o S3.
2. Crea un job serverless de PySpark (por ejemplo, Dataproc Serverless o AWS Glue) cuyo
   entrypoint sea `football_data_scraper.py` (la función `spark_main` también está
   disponible si el servicio la requiere).
3. Programa la ejecución diaria a las 05:00 con Cloud Scheduler, EventBridge o el
   servicio equivalente de tu proveedor.

Cada ejecución sobrescribe los archivos existentes, manteniendo el dataset actualizado
sin pasos adicionales.
