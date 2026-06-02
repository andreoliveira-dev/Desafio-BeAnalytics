import os
import urllib.parse
import polars as pl
from gold.ports.output_ports import MetricsWriterPort
from bronze.adapters.s3_storage_adapter import get_s3_storage_options, ensure_s3_bucket


class ParquetMetricsWriterAdapter(MetricsWriterPort):
    def __init__(self, file_path: str = None, output_dir: str = "data/gold"):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = os.path.join(output_dir, "selic_metrics.parquet")

    def write_metrics(self, df: pl.DataFrame) -> str:
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

            if "ano" in df.columns and "mes" in df.columns:
                s3_dir = os.path.dirname(self.file_path)
                for (yr, mn), sub_df in df.group_by(["ano", "mes"]):
                    part_path = f"{s3_dir}/partitioned/year={yr}/month={mn}/data.parquet"
                    with fs.open(part_path, "wb") as f:
                        sub_df.write_parquet(f)
        else:
            output_dir = os.path.dirname(self.file_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            df.write_parquet(self.file_path)

            if "ano" in df.columns and "mes" in df.columns:
                local_dir = os.path.dirname(self.file_path)
                for (yr, mn), sub_df in df.group_by(["ano", "mes"]):
                    part_dir = os.path.join(
                        local_dir, "partitioned", f"year={yr}", f"month={mn}"
                    )
                    os.makedirs(part_dir, exist_ok=True)
                    part_path = os.path.join(part_dir, "data.parquet")
                    sub_df.write_parquet(part_path)

        return self.file_path
