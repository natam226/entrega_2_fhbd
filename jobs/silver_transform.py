import logging
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import month

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)


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
    """Limpia y enriquece el DataFrame de votes usando Spark SQL."""
    try:
        logging.info("🧹 Limpiando el dataset VOTES...")
        df.createOrReplaceTempView("raw_votes")

        # Calcular mediana solo para votos con bounty (8 y 9)
        median_bounty = 0
        bounty_df = df.filter("vote_type_id IN (8,9) AND bounty_amount IS NOT NULL")
        if bounty_df.count() > 0:
            median_bounty = bounty_df.approxQuantile("bounty_amount", [0.5], 0.1)[0]

        cleaned_df = spark.sql(f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY creation_date DESC) AS rn
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
                CASE 
                    WHEN vote_type_id IN (5,8) THEN COALESCE(user_id, -1)
                    ELSE -1
                END AS user_id_clean,
                CASE 
                    WHEN vote_type_id IN (8,9) THEN COALESCE(bounty_amount, {median_bounty})
                    ELSE NULL
                END AS bounty_amount_clean
            FROM ranked
            WHERE rn = 1
        """)
        logging.info("✅ Limpieza de VOTES completada.")
        return cleaned_df
    except Exception as e:
        logging.error(f"❌ Error limpiando VOTES: {e}")
        raise


def clean_posts_sql(spark: SparkSession, df):
    """Limpia y enriquece el DataFrame de posts usando Spark SQL."""
    try:
        logging.info("🧹 Limpiando el dataset POSTS...")
        df.createOrReplaceTempView("raw_posts")

        cleaned_df = spark.sql("""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY Id
                           ORDER BY COALESCE(LastActivityDate, CreationDate) DESC
                       ) AS rn
                FROM raw_posts
                WHERE Id NOT IN (1000000001, 1000000010)
            ),
            casted AS (
                SELECT
                    Id,
                    CAST(PostTypeId AS INT) AS PostTypeId,
                    CAST(AcceptedAnswerId AS STRING) AS AcceptedAnswerId_clean,
                    TO_TIMESTAMP(CreationDate) AS CreationDate,
                    TO_TIMESTAMP(LastEditDate) AS LastEditDate,
                    TO_TIMESTAMP(LastActivityDate) AS LastActivityDate,
                    TO_TIMESTAMP(ClosedDate) AS ClosedDate,
                    TO_TIMESTAMP(CommunityOwnedDate) AS CommunityOwnedDate,
                    CAST(OwnerUserId AS STRING) AS OwnerUserId_clean,
                    CAST(OwnerDisplayName AS STRING) AS OwnerDisplayName,
                    CAST(LastEditorUserId AS STRING) AS LastEditorUserId,
                    CAST(LastEditorDisplayName AS STRING) AS LastEditorDisplayName,
                    CAST(Score AS BIGINT) AS Score,
                    CAST(ViewCount AS BIGINT) AS ViewCount,
                    CAST(AnswerCount AS BIGINT) AS AnswerCount,
                    CAST(CommentCount AS BIGINT) AS CommentCount,
                    CAST(FavoriteCount AS BIGINT) AS FavoriteCount,
                    CAST(Body AS STRING) AS Body,
                    CAST(Title AS STRING) AS Title,
                    CAST(Tags AS STRING) AS Tags,
                    CAST(ContentLicense AS STRING) AS ContentLicense,
                    CAST(ParentId AS STRING) AS ParentId_clean
                FROM ranked
                WHERE rn = 1 AND Id IS NOT NULL
            ),
            enriched AS (
                SELECT *,
                       DATE_FORMAT(CreationDate, 'yyyy-MM-dd') AS creation_date_str,
                       YEAR(CreationDate) AS year,
                       CURRENT_TIMESTAMP() AS load_date
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
    """Combina, deduplica y fusiona DataFrames en una tabla Iceberg destino."""
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
        logging.error(f"❌ Error al fusionar datos en '{table_name}': {e}")
        raise


def main():
    """Flujo principal de ejecución del script."""
    # La sesión de Spark es creada por el SparkSubmitOperator
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logging.info("Listando archivos Parquet desde la capa Bronze...")
        posts_paths = list_parquet_files(spark, "s3a://bronze/posts/")
        votes_paths = list_parquet_files(spark, "s3a://bronze/votes/")

        if not posts_paths or not votes_paths:
            logging.error("No se encontraron archivos Parquet en 'posts' o 'votes'. Abortando.")
            return

        logging.info("Filtrando y leyendo datos por año...")
        votes_2022 = spark.read.parquet(*filter_by_year(votes_paths, 2022))
        votes_2023 = spark.read.parquet(*filter_by_year(votes_paths, 2023))
        posts_2022 = spark.read.parquet(*filter_by_year(posts_paths, 2022))
        posts_2023 = spark.read.parquet(*filter_by_year(posts_paths, 2023))

        posts_2022_clean = clean_posts_sql(spark, posts_2022)
        posts_2023_clean = clean_posts_sql(spark, posts_2023)
        votes_2022_clean = clean_votes_sql(spark, votes_2022)
        votes_2023_clean = clean_votes_sql(spark, votes_2023)

        logging.info("Asegurando la existencia del namespace 'nessie.silver'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

        logging.info("Filtrando los datos limpios para los primeros 4 meses...")
        filtered_posts_2022 = posts_2022_clean.filter(month("CreationDate").between(1, 4))
        filtered_posts_2023 = posts_2023_clean.filter(month("CreationDate").between(1, 4))
        filtered_votes_2022 = votes_2022_clean.filter(month("creation_ts").between(1, 4))
        filtered_votes_2023 = votes_2023_clean.filter(month("creation_ts").between(1, 4))

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


if __name__ == "__main__":
    main()