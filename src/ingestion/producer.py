import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List


def generate_order_record(inject_corruption: bool = False) -> Dict[str, Any]:
    """Genera un registro individual de orden con soporte para inyección de anomalías."""
    statuses = ["placed", "preparing", "delivered", "cancelled"]
    countries = ["CO", "MX", "BR", "CL"]

    record = {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 99999),
        "country_code": random.choice(countries),
        "total_amount": round(random.uniform(5.0, 150.0), 2),
        "order_status": random.choice(statuses),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Inyección de fallos sintéticos: 5% de registros corruptos
    if inject_corruption and random.random() < 0.05:
        anomaly_type = random.choice(
            ["negative_amount", "invalid_country", "bad_timestamp"]
        )
        if anomaly_type == "negative_amount":
            record["total_amount"] = -50.00  # Violación semántica
        elif anomaly_type == "invalid_country":
            record["country_code"] = "XX"  # Violación de categoría
        elif anomaly_type == "bad_timestamp":
            record["created_at"] = (
                "invalid-iso-date"  # Violación de tipo/formato
            )

    return record


def stream_order_batches(
    total_records: int, batch_size: int = 1000, inject_corruption: bool = True
) -> Generator[List[Dict[str, Any]], None, None]:
    records_yielded = 0
    while records_yielded < total_records:
        current_batch_size = min(batch_size, total_records - records_yielded)
        batch = [
            generate_order_record(inject_corruption=inject_corruption)
            for _ in range(current_batch_size)
        ]
        records_yielded += current_batch_size
        yield batch