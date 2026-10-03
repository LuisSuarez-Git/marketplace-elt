import json
import logging
import sys
from pathlib import Path
from pydantic import ValidationError
from src.ingestion.models import OrderEvent
from src.ingestion.producer import stream_order_batches
from src.ingestion.writer import write_batch_to_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def route_batch(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    """Valida cada registro con el contrato Pydantic.

    Separa los registros válidos de los corruptos (DLQ pattern) garantizando
    que los errores sean serializables.
    """
    valid_records = []
    quarantine_records = []

    for item in batch:
        try:
            validated = OrderEvent(**item)
            valid_records.append(validated.model_dump())
        except ValidationError as err:
            # Sanitizamos los errores para que sean 100% JSON serializables
            clean_errors = []
            for e in err.errors():
                clean_errors.append(
                    {
                        "field": ".".join(str(loc) for loc in e.get("loc", [])),
                        "message": e.get("msg"),
                        "type": e.get("type"),
                    }
                )

            quarantine_records.append(
                {"raw_payload": item, "errors": clean_errors}
            )

    return valid_records, quarantine_records


def run_pipeline(total_records: int = 5_000, batch_size: int = 1_000) -> None:
    logger.info(
        f"Iniciando pipeline con validación y DLQ: {total_records} registros..."
    )

    batch_generator = stream_order_batches(
        total_records=total_records, batch_size=batch_size, inject_corruption=True
    )

    total_valid = 0
    total_quarantined = 0

    for idx, raw_batch in enumerate(batch_generator, start=1):
        valid_orders, invalid_orders = route_batch(raw_batch)

        # 1. Escritura de datos limpios a Parquet
        if valid_orders:
            file_path = write_batch_to_parquet(
                valid_orders, output_base_dir="data/raw/orders"
            )
            total_valid += len(valid_orders)
            logger.info(
                f"Lote {idx}: {len(valid_orders)} registros limpios escritos en {file_path}"
            )

        # 2. Aislamiento de anomalías a Dead-Letter Queue (JSONL)
        if invalid_orders:
            quarantine_path = Path("data/quarantine/corrupted_orders.jsonl")
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            with open(quarantine_path, "a", encoding="utf-8") as f:
                for bad_record in invalid_orders:
                    # Usamos default=str como red de seguridad adicional
                    f.write(json.dumps(bad_record, default=str) + "\n")
            total_quarantined += len(invalid_orders)
            logger.warning(
                f"Lote {idx}: {len(invalid_orders)} registros derivados a DLQ/Cuarentena."
            )

    logger.info(
        f"Pipeline finalizado. Registros válidos: {total_valid} | Cuarentena (DLQ): {total_quarantined}"
    )


if __name__ == "__main__":
    run_pipeline()