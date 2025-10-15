import logging
import sys

from pyspark.sql import SparkSession

# --- Configuración del Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)


def main():
    """Flujo principal para transformación a la capa Gold: agregaciones de votos por post."""
    # La sesión de Spark es creada por el SparkSubmitOperator
    spark = SparkSession.builder.getOrCreate()
    
    try:
        logging.info("Leyendo tablas desde la capa Silver...")
        posts_df = spark.read.table("nessie.silver.posts")
        votes_df = spark.read.table("nessie.silver.votes")

        logging.info("Creando vistas temporales...")
        posts_df.createOrReplaceTempView("posts_view")
        votes_df.createOrReplaceTempView("votes_view")

        logging.info("Ejecutando consulta SQL para agregaciones de votos por post...")
        vote_stats_per_post = spark.sql("""
            WITH valid_votes AS (
                SELECT
                    v.post_id_clean AS post_id,
                    v.vote_type_id_clean,
                    v.creation_ts,
                    v.creation_year,
                    DATE_FORMAT(v.creation_ts, 'yyyy-MM') AS year_month
                FROM votes_view v
                WHERE v.post_id_clean IS NOT NULL AND v.post_id_clean != -1
                      AND v.vote_type_id_clean != -1
            ),
            vote_summary AS (
                SELECT
                    post_id,
                    YEAR(creation_ts) AS year,
                    MONTH(creation_ts) AS month,
                    SUM(CASE WHEN vote_type_id_clean IN (2, 18, 20, 21, 32) THEN 1 ELSE 0 END) AS upvotes,
                    SUM(CASE WHEN vote_type_id_clean IN (3, 33) THEN 1 ELSE 0 END) AS downvotes,
                    COUNT(*) AS total_votes
                FROM valid_votes
                GROUP BY post_id, YEAR(creation_ts), MONTH(creation_ts)
            ),
            posts_enriched AS (
                SELECT
                    p.Id AS post_id,
                    p.Score AS base_score,
                    p.ViewCount,
                    p.AnswerCount,
                    p.CommentCount,
                    p.FavoriteCount,
                    p.PostTypeId,
                    p.CreationDate,
                    p.LastActivityDate,
                    CASE p.PostTypeId
                        WHEN 1 THEN 'Question'
                        WHEN 2 THEN 'Answer'
                        WHEN 3 THEN 'Orphaned Tag Wiki'
                        WHEN 4 THEN 'Tag Wiki Excerpt'
                        WHEN 5 THEN 'Tag Wiki'
                        WHEN 6 THEN 'Moderator Nomination'
                        WHEN 7 THEN 'Wiki Placeholder'
                        WHEN 8 THEN 'Privilege Wiki'
                        WHEN 9 THEN 'Article'
                        WHEN 10 THEN 'Help Article'
                        WHEN 12 THEN 'Collection'
                        WHEN 13 THEN 'Moderator Questionnaire Response'
                        WHEN 14 THEN 'Announcement'
                        WHEN 15 THEN 'Collective Discussion'
                        WHEN 17 THEN 'Collective Collection'
                        ELSE 'Other'
                    END AS post_type
                FROM posts_view p
            )
            SELECT
                ps.post_id,
                ps.post_type,
                vs.year,
                vs.month,
                ps.ViewCount,
                ps.AnswerCount,
                ps.CommentCount,
                ps.FavoriteCount,
                COALESCE(vs.upvotes, 0) AS upvotes,
                COALESCE(vs.downvotes, 0) AS downvotes,
                COALESCE(vs.total_votes, 0) AS total_votes,
                (ps.base_score + COALESCE(vs.upvotes, 0) - COALESCE(vs.downvotes, 0)) AS score,
                ROUND(
                    CASE WHEN vs.total_votes > 0 THEN vs.upvotes / vs.total_votes ELSE 0 END, 3
                ) AS upvote_pct,
                ROUND(
                    CASE WHEN vs.total_votes > 0 THEN vs.downvotes / vs.total_votes ELSE 0 END, 3
                ) AS downvote_pct,
                CURRENT_TIMESTAMP() AS load_date
            FROM posts_enriched ps
            LEFT JOIN vote_summary vs
                ON ps.post_id = vs.post_id
            WHERE vs.year = 2023 AND vs.month IS NOT NULL
        """)

        logging.info("Asegurando la existencia del namespace 'nessie.gold'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

        logging.info("Registrando vista temporal para MERGE...")
        vote_stats_per_post.createOrReplaceTempView("vote_stats_updates")

        table_path = "nessie.gold.vote_stats_per_post"
        table_exists = spark.catalog.tableExists(table_path)

        if not table_exists:
            logging.info(f"⚙️ La tabla {table_path} no existe. Creándola...")
            vote_stats_per_post.writeTo(table_path).create()
            logging.info(f"✅ Tabla {table_path} creada exitosamente.")
        else:
            logging.info(f"Fusionando datos en la tabla existente {table_path}...")
            spark.sql(f"""
                MERGE INTO {table_path} AS target
                USING vote_stats_updates AS source
                ON target.post_id = source.post_id 
                   AND target.year = source.year 
                   AND target.month = source.month
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            logging.info(f"✅ Datos fusionados exitosamente en {table_path} usando MERGE de Iceberg con granularidad por fecha.")

        # Opcional: Verificar conteo para logging
        count = vote_stats_per_post.count()
        logging.info(f"Filas procesadas: {count}")

        logging.info("✅ Proceso de transformación a la capa Gold completado exitosamente.")
    except Exception as e:
        logging.error(f"Ha ocurrido un error en el proceso principal: {e}")
        raise
    finally:
        logging.info("Cerrando la sesión de Spark.")
        spark.stop()


if __name__ == "__main__":
    main()