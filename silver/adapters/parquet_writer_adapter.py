import os
import urllib.parse
import polars as pl
from silver.ports.output_ports import CleanDataWriterPort
from bronze.adapters.s3_storage_adapter import get_s3_storage_options, ensure_s3_bucket


class ParquetCleanWriterAdapter(CleanDataWriterPort):
    def __init__(self, file_path: str = None, output_dir: str = "data/silver"):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = os.path.join(output_dir, "selic_cleaned.parquet")

    def write_clean_data(self, df: pl.DataFrame) -> str:
        if self.file_path.startswith("s3://"):
            parsed = urllib.parse.urlparse(self.file_path)
            bucket_name = parsed.netloc if parsed.netloc else "selic-bucket"
            ensure_s3_bucket(bucket_name)

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


        else:
            output_dir = os.path.dirname(self.file_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            df.write_parquet(self.file_path)

        return self.file_path

