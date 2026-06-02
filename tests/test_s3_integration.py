import os
import pytest
import polars as pl
import boto3
from bronze.adapters.s3_storage_adapter import S3ParquetStorageAdapter, get_s3_storage_options
from bronze.domain.models import SelicRawRecord
from silver.adapters.parquet_reader_adapter import ParquetRawReaderAdapter
from silver.adapters.parquet_writer_adapter import ParquetCleanWriterAdapter
from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter

endpoint_url = os.getenv("S3_ENDPOINT_URL")
has_s3 = endpoint_url is not None

if has_s3:
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        s3_client.list_buckets()
    except Exception:
        has_s3 = False

pytestmark = pytest.mark.skipif(not has_s3, reason="S3/MinIO integration environment not available")


def test_s3_end_to_end_integration():
    bucket_name = "integration-test-bucket"

    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    bronze_path = f"s3://{bucket_name}/bronze/selic_raw.parquet"
    silver_path = f"s3://{bucket_name}/silver/selic_cleaned.parquet"
    gold_path = f"s3://{bucket_name}/gold/selic_metrics.parquet"

    raw_records = [
        SelicRawRecord(data="02/01/2020", valor="0.017089"),
        SelicRawRecord(data="03/01/2020", valor="0.017089"),
        SelicRawRecord(data="03/01/2020", valor="0.017089"),
        SelicRawRecord(data="06/01/2020", valor="0.017089")
    ]

    bronze_adapter = S3ParquetStorageAdapter(file_path=bronze_path)
    saved_bronze = bronze_adapter.save_data(raw_records)
    assert saved_bronze == bronze_path

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="bronze/")
    keys = [obj["Key"] for obj in objects.get("Contents", [])]
    assert "bronze/selic_raw.parquet" in keys
    assert "bronze/selic_raw.json" in keys

    raw_reader = ParquetRawReaderAdapter(file_path=bronze_path)
    lazy_raw = raw_reader.read_raw_data()

    cleaned_df = (
        lazy_raw
        .filter(pl.col("data").is_not_null() & pl.col("valor").is_not_null())
        .with_columns([
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y"),
            pl.col("valor").cast(pl.Float64)
        ])
        .unique(subset=["data"])
        .sort("data")
        .collect()
    )

    assert cleaned_df.height == 3

    clean_writer = ParquetCleanWriterAdapter(file_path=silver_path)
    saved_silver = clean_writer.write_clean_data(cleaned_df)
    assert saved_silver == silver_path

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="silver/")
    keys = [obj["Key"] for obj in objects.get("Contents", [])]
    assert "silver/selic_cleaned.parquet" in keys
    assert "silver/partitioned/year=2020/month=1/data.parquet" in keys

    silver_reader = ParquetCleanReaderAdapter(file_path=silver_path)
    lazy_silver = silver_reader.read_clean_data()

    df_temp = lazy_silver.with_columns([
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().alias("mes")
    ])

    metrics_df = (
        df_temp
        .group_by(["ano", "mes"])
        .agg([
            pl.col("valor").mean().alias("media_mensal"),
            pl.col("valor").std().fill_null(0.0).alias("desvio_padrao_mensal"),
            pl.lit(0.0).alias("variacao_mensal"),
            pl.lit(0.0).alias("taxa_acumulada_anual")
        ])
        .collect()
    )

    metrics_writer = ParquetMetricsWriterAdapter(file_path=gold_path)
    saved_gold = metrics_writer.write_metrics(metrics_df)
    assert saved_gold == gold_path

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="gold/")
    keys = [obj["Key"] for obj in objects.get("Contents", [])]
    assert "gold/selic_metrics.parquet" in keys
    assert "gold/partitioned/year=2020/month=1/data.parquet" in keys

    final_df = pl.read_parquet(gold_path, storage_options=get_s3_storage_options())
    assert final_df.height == 1
    assert "media_mensal" in final_df.columns
    assert final_df["ano"][0] == 2020
    assert final_df["mes"][0] == 1
