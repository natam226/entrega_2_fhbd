
# Proyecto ETL con Airflow, Spark, MinIO, Iceberg y Nessie

Este repositorio contiene un pipeline ETL que utiliza **Airflow**, **Apache Spark**, **MinIO**, **Iceberg** y **Nessie** para procesar datos desde un bucket de Bronze hacia Gold con control de versiones y catalogación.

---

## Contenido del repositorio

- `Dockerfile`: Imagen de Airflow con dependencias de Spark, JARs de Iceberg y Nessie, y librerías Python.
- `dags/load_to_iceberg_dag.py`: DAG de Airflow para ejecutar el pipeline ETL.
- `jobs/load_to_iceberg.py`: Código PySpark que realiza la lectura desde Bronze y escritura en Iceberg.
- `.gitignore`: Ignora checkpoints de Jupyter y scripts locales.

---

## Requisitos

- Docker y Docker Compose
- Python 3.10+
- Git
- Acceso a un bucket MinIO (local o remoto)

---

## Instrucciones de uso

### 0. Preparar datos en Bronze

Antes de ejecutar el DAG, asegúrate de subir el archivo de ejemplo al bucket bronze usando la interfaz de MinIO:

- Archivo de ejemplo: [yellow-tripdata-2025-01-parquet](https://www.kaggle.com/datasets/anujchincholikar/yellow-tripdata-2025-01-parquet)
- Esto se hace a través de la UI de MinIO (no por código), en la carpeta correspondiente a Bronze.

### 1. Clonar el repositorio
```bash
git clone <url-del-repo>
cd <nombre-del-repo>
````

### 2. Construir la imagen de Airflow

```bash
docker build -t airflow-spark-minio-iceberg .
```

### 3. Levantar servicios con Docker Compose

Asegúrate de tener un `docker-compose.yml` configurado con:

* Airflow
* MinIO
* Nessie
* (Opcional) PostgreSQL para metadatos de Airflow

```bash
docker-compose up -d --build
```

### 4. Configurar variables de entorno

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
```

* Configura MinIO en `load_to_iceberg_dag.py` y `load_to_iceberg.py` para que apunte a tu endpoint local (`http://minio:9000`).

### 5. Ejecutar el DAG

* Accede a Airflow en `http://localhost:8080`
* Activa el DAG `load_to_iceberg_dag`
* Ejecuta el DAG manualmente o según la programación definida.

---

## Funcionalidades principales

1. **Dockerfile**

   * Incluye todos los JARs necesarios (`scala-library`, `hadoop-aws`, `aws-java-sdk-bundle`, Iceberg runtime, Nessie extensions, PostgreSQL).
   * Instala librerías Python: `pyspark`, `boto3`, `apache-airflow-providers-apache-spark`.
   * Corrige permisos de `/opt/spark/jars`.

2. **DAG**

   * Usa `spark.jars` locales para SparkSubmitOperator.
   * Configuración completa para MinIO + Iceberg + Nessie.
   * Variables de entorno para AWS y MinIO.

3. **Job PySpark**

   * `ensure_bucket_exists()`: crea el bucket Gold si no existe.
   * `drop_table_if_exists()`: elimina tablas Iceberg previas.
   * Flujo ETL: validar bucket → leer Bronze → eliminar tabla si existe → escribir Gold.
   * Logging detallado y manejo de errores comunes (NotFoundException, RuntimeIOException).

---

## Git

* Ignora checkpoints de Jupyter y scripts locales:

```gitignore
.ipynb_checkpoints/
scripts/
```



