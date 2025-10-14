import logging
import re
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import month

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)


# --- Constantes de Configuración ---
CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://silver/"
STORAGE_URI = "http://minio:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"


def create_spark_session() -> SparkSession:
    """Configura e inicializa la sesión de Spark."""
    logging.info("Configurando la sesión de Spark...")

    conf = (
        pyspark.SparkConf()
        .setAppName('silver_transform_task')
        # 📦 Dependencias necesarias
        .set("spark.jars.packages", ",".join([
            "org.postgresql:postgresql:42.7.3",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
            "software.amazon.awssdk:bundle:2.24.8",
            "software.amazon.awssdk:url-connection-client:2.24.8",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ]))
        # 🧩 Extensiones Iceberg + Nessie
        .set("spark.sql.extensions", ",".join([
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ]))
        # 🗂️ Catálogo Nessie
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        # ☁️ Configuración S3A para MinIO
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ⚡ Optimizaciones de ejecución
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.parquet.filterPushdown", "true")
        .set("spark.sql.parquet.mergeSchema", "false")
        .set("spark.sql.shuffle.partitions", "64")
        .set("spark.sql.files.maxPartitionBytes", "64MB")
        .set("spark.driver.memory", "5g")
        .set("spark.executor.memory", "6g")
        .set("spark.executor.cores", "4")
        .set("spark.driver.maxResultSize", "2g")
        .set("spark.network.timeout", "600s")
        .set("spark.executor.heartbeatInterval", "60s")
        # ⚙️ Escritura
        .set("spark.sql.parquet.compression.codec", "snappy")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logging.info("Sesión de Spark iniciada correctamente.")
    return spark


def list_parquet_files(spark: SparkSession, base_path: str) -> list:
    """Lista recursivamente todos los archivos .parquet en una ruta S3A."""
    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark._jvm, 'java.net.URI')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(base_path),
        spark._jsc.hadoopConfiguration()
    )

    results = []
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)

    if not fs.exists(path):
        logging.warning(f"La ruta no existe: {base_path}")
        return results

    def recurse(p):
        try:
            for f in fs.listStatus(p):
                if f.isDirectory():
                    recurse(f.getPath())
                elif f.getPath().getName().endswith(".parquet"):
                    results.append(f.getPath().toString())
        except Exception as e:
            logging.error(f"Error listando {p}: {e}")

    recurse(path)
    return results


def filter_by_year(paths: list, year: int) -> list:
    """Filtra una lista de rutas por un año específico."""
    pattern = re.compile(rf"[_/\\.]{year}([_/\\.]|$)")
    return [p for p in paths if pattern.search(p)]


def clean_votes_sql(spark: SparkSession, df):
    """Limpia y enriquece el DataFrame de votos usando Spark SQL."""
    try:
        logging.info("🧹 Limpiando y enriqueciendo el dataset VOTES con Spark SQL...")
        df.createOrReplaceTempView("raw_votes")

        median_bounty = 0
        bounty_df = df.filter("vote_type_id IN (8,9) AND bounty_amount IS NOT NULL")
        if bounty_df.count() > 0:
            median_bounty = bounty_df.approxQuantile("bounty_amount", [0.5], 0.1)[0]

        cleaned_df = spark.sql(f"""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY creation_date DESC) AS rn
                FROM raw_votes
                WHERE id IS NOT NULL AND creation_date IS NOT NULL
            )
            SELECT
                id,
                COALESCE(post_id, -1) AS post_id_clean,
                COALESCE(vote_type_id, -1) AS vote_type_id_clean,
                TO_TIMESTAMP(creation_date) AS creation_ts,
                DATE_FORMAT(TO_TIMESTAMP(creation_date), 'yyyy-MM-dd') AS creation_date_str,
                YEAR(TO_TIMESTAMP(creation_date)) AS creation_year,
                CURRENT_TIMESTAMP() AS load_date,
                CASE WHEN vote_type_id IN (5,8) THEN COALESCE(user_id, -1) ELSE -1 END AS user_id_clean,
                CASE WHEN vote_type_id IN (8,9) THEN COALESCE(bounty_amount, {median_bounty}) ELSE NULL END AS bounty_amount_clean
            FROM ranked WHERE rn = 1
        """)
        logging.info("✅ Limpieza de VOTES completada.")
        return cleaned_df
    except Exception as e:
        logging.error(f"❌ Error limpiando VOTES con Spark SQL: {e}")
        raise


def clean_posts_sql(spark: SparkSession, df):
    """Limpia y enriquece el DataFrame de posts usando Spark SQL."""
    try:
        logging.info("🧹 Limpiando el dataset POSTS...")
        df.createOrReplaceTempView("raw_posts")

        cleaned_df = spark.sql("""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY Id ORDER BY COALESCE(last_activity_date, creation_date) DESC) AS rn
                FROM raw_posts WHERE Id NOT IN (1000000001, 1000000010)
            ),
            casted AS (
                SELECT
                    Id, CAST(post_type_id AS INT) AS PostTypeId, CAST(accepted_answer_id AS STRING) AS AcceptedAnswerId_clean,
                    TO_TIMESTAMP(creation_date) AS CreationDate, TO_TIMESTAMP(last_edit_date) AS LastEditDate,
                    TO_TIMESTAMP(last_activity_date) AS LastActivityDate, TO_TIMESTAMP(closed_date) AS ClosedDate,
                    TO_TIMESTAMP(community_owned_date) AS CommunityOwnedDate, CAST(owner_user_id AS STRING) AS OwnerUserId_clean,
                    CAST(owner_display_name AS STRING) AS OwnerDisplayName, CAST(last_editor_user_id AS STRING) AS LastEditorUserId,
                    CAST(last_editor_display_name AS STRING) AS LastEditorDisplayName, CAST(score AS BIGINT) AS Score,
                    CAST(view_count AS BIGINT) AS ViewCount, CAST(answer_count AS BIGINT) AS AnswerCount,
                    CAST(comment_count AS BIGINT) AS CommentCount, CAST(favorite_count AS BIGINT) AS FavoriteCount,
                    CAST(body AS STRING) AS Body, CAST(title AS STRING) AS Title, CAST(tags AS STRING) AS Tags,
                    CAST(content_license AS STRING) AS ContentLicense, CAST(parent_id AS STRING) AS ParentId_clean
                FROM ranked WHERE rn = 1 AND Id IS NOT NULL
            ),
            enriched AS (
                SELECT *, DATE_FORMAT(CreationDate, 'yyyy-MM-dd') AS creation_date_str,
                       YEAR(CreationDate) AS year, CURRENT_TIMESTAMP() AS load_date
                FROM casted
            )
            SELECT * FROM enriched
        """)
        logging.info("✅ Limpieza de POSTS completada.")
        return cleaned_df
    except Exception as e:
        logging.error(f"❌ Error limpiando POSTS: {e}")
        raise


def merge_table_sql(spark: SparkSession, df_2022, df_2023, date_col: str, id_col: str, catalog: str, table_name: str):
    """
    Combina, deduplica y fusiona DataFrames en una tabla Iceberg destino.
    Soporta tanto 'posts' como 'votes'.
    """
    try:
        view_2022 = f"{table_name}_2022"
        view_2023 = f"{table_name}_2023"
        updates_view = f"{table_name}_updates"
        table_path = f"{catalog}.{table_name}"
        
        df_2022.createOrReplaceTempView(view_2022)
        df_2023.createOrReplaceTempView(view_2023)

        logging.info(f"Creando vista combinada y deduplicada '{updates_view}'...")
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {updates_view} AS
            WITH combined AS (
                SELECT * FROM {view_2022}
                UNION ALL
                SELECT * FROM {view_2023}
            ),
            ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY {date_col} DESC) AS rank
                FROM combined
            )
            SELECT * FROM ranked WHERE rank = 1
        """)

        table_exists = spark.catalog.tableExists(table_path)

        if not table_exists:
            logging.info(f"⚙️ La tabla {table_path} no existe. Creándola...")
            spark.table(updates_view).writeTo(table_path).create()
            logging.info(f"✅ Tabla {table_path} creada exitosamente.")
        else:
            logging.info(f"Fusionando datos en la tabla existente {table_path}...")
            spark.sql(f"""
                MERGE INTO {table_path} AS target
                USING {updates_view} AS source
                ON target.{id_col} = source.{id_col}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            logging.info(f"✅ Datos fusionados exitosamente en {table_path} usando MERGE de Iceberg.")

    except Exception as e:
        # Nota: Los errores en el notebook original (Py4JNetworkError) sugieren problemas de
        # conexión o recursos con el clúster de Spark. Este error se propagará.
        logging.error(f"❌ Error al fusionar datos en '{table_name}': {e}")
        raise

def main():
    """Flujo principal de ejecución del script."""
    spark = create_spark_session()
    
    try:
        # 1. Leer rutas de la capa Bronze
        logging.info("Listando archivos Parquet desde la capa Bronze...")
        posts_paths = list_parquet_files(spark, "s3a://bronze/posts/")
        votes_paths = list_parquet_files(spark, "s3a://bronze/votes/")

        if not posts_paths or not votes_paths:
            logging.error("No se encontraron archivos Parquet en 'posts' o 'votes'. Abortando.")
            return

        # 2. Filtrar rutas por año y leer en DataFrames
        logging.info("Filtrando y leyendo datos por año...")
        votes_2022 = spark.read.parquet(*filter_by_year(votes_paths, 2022))
        votes_2023 = spark.read.parquet(*filter_by_year(votes_paths, 2023))
        posts_2022 = spark.read.parquet(*filter_by_year(posts_paths, 2022))
        posts_2023 = spark.read.parquet(*filter_by_year(posts_paths, 2023))

        # 3. Limpiar y enriquecer los DataFrames
        posts_2022_clean = clean_posts_sql(spark, posts_2022)
        posts_2023_clean = clean_posts_sql(spark, posts_2023)
        votes_2022_clean = clean_votes_sql(spark, votes_2022)
        votes_2023_clean = clean_votes_sql(spark, votes_2023)

        # 4. Crear Namespace en Nessie si no existe
        logging.info("Asegurando la existencia del namespace 'nessie.silver'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

        # 5. Filtrar datos limpios por los primeros 4 meses
        logging.info("Filtrando los datos limpios para los primeros 4 meses...")
        filtered_posts_2022 = posts_2022_clean.filter(month("CreationDate").between(1, 3))
        filtered_posts_2023 = posts_2023_clean.filter(month("CreationDate").between(1, 3))
        filtered_votes_2022 = votes_2022_clean.filter(month("creation_ts").between(1, 3))
        filtered_votes_2023 = votes_2023_clean.filter(month("creation_ts").between(1, 3))

        # 6. Fusionar datos en las tablas Silver
        merge_table_sql(
            spark=spark, df_2022=filtered_posts_2022, df_2023=filtered_posts_2023,
            date_col="LastActivityDate", id_col="Id",
            catalog="nessie.silver", table_name="posts"
        )
        
        merge_table_sql(
            spark=spark, df_2022=filtered_votes_2022, df_2023=filtered_votes_2023,
            date_col="creation_ts", id_col="id",
            catalog="nessie.silver", table_name="votes"
        )
        
        logging.info("✅ Proceso de transformación a la capa Silver completado exitosamente.")

    except Exception as e:
        logging.error(f"Ha ocurrido un error en el proceso principal: {e}")
        raise
    finally:
        logging.info("Cerrando la sesión de Spark.")
        spark.stop()
