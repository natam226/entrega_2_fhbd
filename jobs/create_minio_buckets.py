import boto3
from botocore.exceptions import ClientError

def create_minio_buckets(
    endpoint_url="http://minio:9000",
    access_key="admin",
    secret_key="password",
    buckets=None
):
    """
    Crea buckets en un servidor MinIO usando boto3.
    Si el bucket ya existe, lo omite.

    Parameters
    ----------
    endpoint_url : str
        URL del endpoint MinIO
    access_key : str
        Usuario o access key del MinIO
    secret_key : str
        Contraseña o secret key del MinIO
    buckets : list[str]
        Lista de nombres de buckets a crear
    """
    if buckets is None:
        buckets = ["bronze", "silver", "gold"]

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )

    for bucket_name in buckets:
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"ℹ️ El bucket '{bucket_name}' ya existe.")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "400", "NoSuchBucket"):
                try:
                    s3.create_bucket(Bucket=bucket_name)
                    print(f"✅ Bucket '{bucket_name}' creado exitosamente.")
                except ClientError as ce:
                    print(f"❌ Error creando bucket '{bucket_name}': {ce}")
                    raise
            else:
                print(f"⚠️ Error verificando bucket '{bucket_name}': {e}")

def main():
    create_minio_buckets(
        buckets=["bronze", "silver", "gold"]
    )