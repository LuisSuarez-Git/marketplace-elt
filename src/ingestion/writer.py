from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import pyarrow as pa
import pyarrow.parquet as pq


# 1. Definimos un esquema estricto (Strict Schema)
# Esto garantiza calidad de datos antes de que los archivos toquen el disco o S3
ORDER_SCHEMA = pa.schema(
    [
        ("order_id", pa.string()),
        ("user_id", pa.int64()),
        ("country_code", pa.string()),
        ("total_amount", pa.float64()),
        ("order_status", pa.string()),
        ("created_at", pa.string()),
    ]
)


def write_batch_to_parquet(
    batch: List[Dict[str, Any]], output_base_dir: str = "data/raw"
) -> Path:
    """Toma un lote en memoria, lo convierte a tabla PyArrow con tipado estricto

    y lo escribe en una estructura de particionado por fecha (year/month/day).
    """
    if not batch:
        raise ValueError("No se puede escribir un lote vacío.")

    # Convertimos la lista de diccionarios a una tabla columnar en memoria
    table = pa.Table.from_pylist(batch, schema=ORDER_SCHEMA)

    # Extraemos la fecha del primer registro para armar la ruta de partición
    # (Simulando la partición de almacenamiento en S3: year=YYYY/month=MM/day=DD)
    timestamp_sample = datetime.fromisoformat(batch[0]["created_at"])
    year = timestamp_sample.strftime("%Y")
    month = timestamp_sample.strftime("%m")
    day = timestamp_sample.strftime("%d")

    # Construimos la carpeta local de destino
    partition_dir = (
        Path(output_base_dir)
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Generamos un nombre de archivo único para evitar sobreescrituras (idempotencia)
    file_id = batch[0]["order_id"][:8]
    file_path = partition_dir / f"orders_part_{file_id}.parquet"

    # Escribimos el archivo Parquet con compresión Snappy
    pq.write_table(table, file_path, compression="snappy")

    return file_path