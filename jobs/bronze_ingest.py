import boto3
from aiohttp import ClientTimeout
import dlt
import fsspec
import logging
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ensure_bucket_exists(endpoint, access_key, secret_key, bucket_name="bronze"):
    logger.info(f"🔍 Verificando bucket `{bucket_name}` en MinIO...")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"🆕 Bucket `{bucket_name}` creado en MinIO")
    else:
        logger.info(f"✅ Bucket `{bucket_name}` ya existe en MinIO")

def load_parquet_to_minio(parquet_url, batch_size=50000):
    """
    Downloads and processes a remote Parquet file in batches, yielding pandas DataFrames.

    This version is optimized for single Parquet files (not directory-partitioned).
    It uses `fsspec` to stream the file over HTTPS and `pyarrow.parquet.ParquetFile`
    to read it efficiently in row groups or batches. Ideal for scalable ingestion
    in data pipelines like DLT, especially when loading into destinations such as MinIO.

    Args:
        parquet_url (str): Full URL of the remote Parquet file.
        batch_size (int, optional): Number of rows per batch. Default is 50,000.

    Yields:
        pandas.DataFrame: A batch of rows converted from Parquet to pandas format.

    Example:
        >>> for df in load_parquet_to_minio("https://.../posts/2022.parquet"):
        >>>     process(df)
    """
    timeout = ClientTimeout(total=600)
    fs = fsspec.filesystem("https", client_kwargs={"timeout": timeout})

    with fs.open(parquet_url) as f:
        parquet_file = pq.ParquetFile(f)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            yield batch.to_pandas()

# Recursos DLT
@dlt.resource(table_name="votes_2022")
def votes_2022():
    url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/2022.parquet"
    yield from load_parquet_to_minio(url)

@dlt.resource(table_name="votes_2023")
def votes_2023():
    url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/2023.parquet"
    yield from load_parquet_to_minio(url)

def load_votes_table():

    print("📖 bucket_url:", dlt.secrets.get("parquet_to_minio.destination.filesystem.bucket_url"))

    pipeline_votes = dlt.pipeline(
        pipeline_name="parquet_to_minio",
        destination="filesystem",
        dataset_name="votes",
    )

    # VOTES 2022
    try:
        load_info = pipeline_votes.run(
            votes_2022(),
            table_name="2022",
            loader_file_format="parquet",
            write_disposition="replace"
        )
        print("✅ votes/2022 loaded:", load_info)
    except Exception as e:
        return f"❌ Error loading votes/2022: {e}"
    # VOTES 2023
    try:
        load_info = pipeline_votes.run(
            votes_2023(),
            table_name="2023",
            loader_file_format="parquet",
            write_disposition="replace"
        )
        print("✅ votes/2023 loaded:", load_info)
    except Exception as e:
        return f"❌ Error loading votes/2023: {e}"
    
    return None
    
def main():

    logger.info("🚀 Starting data ingestion process...")
    try:
        ensure_bucket_exists(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key="password"
        )

        load_state = load_votes_table()

        if load_state is not None:
            logger.error(load_state)
            raise
        
        logger.info("✅ Data ingestion completed successfully.")
        return True
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise