import os
import urllib.parse
from typing import List
import polars as pl
import boto3
from bronze.ports.output_ports import RawStoragePort
from bronze.domain.models import SelicRawRecord


def get_s3_storage_options() -> dict:
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    options = {}
    if access_key:
        options["aws_access_key_id"] = access_key
    if secret_key:
        options["aws_secret_access_key"] = secret_key
    if endpoint_url:
        options["endpoint_url"] = endpoint_url
    if region:
        options["aws_region"] = region
    return options


def ensure_s3_bucket(bucket_name: str) -> None:
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )


class S3ParquetStorageAdapter(RawStoragePort):
    def __init__(self, file_path: str = None):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = "s3://selic-bucket/bronze/selic_raw.parquet"

        parsed = urllib.parse.urlparse(self.file_path)
        self.bucket_name = parsed.netloc if parsed.netloc else "selic-bucket"

    def save_data(self, records: List[SelicRawRecord]) -> str:
        ensure_s3_bucket(self.bucket_name)

        data = [{"data": r.data, "valor": r.valor} for r in records]
        df = pl.DataFrame(data)

        import s3fs
        opts = get_s3_storage_options()
        s3fs_args = {}
        if "aws_access_key_id" in opts:
            s3fs_args["key"] = opts["aws_access_key_id"]
        if "aws_secret_access_key" in opts:
            s3fs_args["secret"] = opts["aws_secret_access_key"]
        if "endpoint_url" in opts:
            s3fs_args["endpoint_url"] = opts["endpoint_url"]

        fs = s3fs.S3FileSystem(**s3fs_args)
        with fs.open(self.file_path, "wb") as f:
            df.write_parquet(f)

        import json
        json_path = self.file_path.replace(".parquet", ".json")
        with fs.open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return self.file_path
