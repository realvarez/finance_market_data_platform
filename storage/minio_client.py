import io
import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from ingestion import config

logger = logging.getLogger(__name__)


def get_client() -> Minio:
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )


def ensure_bucket(client: Minio | None = None) -> None:
    client = client or get_client()
    if not client.bucket_exists(config.MINIO_BUCKET):
        client.make_bucket(config.MINIO_BUCKET)
        logger.info("Created bucket %s", config.MINIO_BUCKET)


def _partition_path(data_type: str, symbol: str, timestamp: str, **extra) -> str:
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    parts = extra.get("prefix", f"raw/{data_type}")
    path = f"{parts}/symbol={symbol}/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
    if "interval" in extra:
        path += f"/interval={extra['interval']}"
    if "source" in extra:
        path += f"/source={extra['source']}"
    filename = f"{dt.strftime('%H%M%S')}_{extra.get('event_id', 'data')}.parquet"
    return f"{path}/{filename}"


def write_parquet(records: list[dict], data_type: str, **extra) -> None:
    if not records:
        return

    client = get_client()
    ensure_bucket(client)

    for record in records:
        symbol = record.get("symbol", "UNKNOWN")
        timestamp = record.get("timestamp", datetime.utcnow().isoformat())
        object_path = _partition_path(
            data_type, symbol, timestamp, event_id=record.get("event_id", ""), **extra
        )

        table = pa.Table.from_pylist([record])
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        client.put_object(
            config.MINIO_BUCKET,
            object_path,
            buf,
            length=buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.debug("Wrote %s to MinIO", object_path)
