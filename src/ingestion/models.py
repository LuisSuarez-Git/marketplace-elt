from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator


class OrderEvent(BaseModel):
    order_id: str
    user_id: int = Field(gt=0, description="El user_id debe ser un entero positivo")
    country_code: Literal["CO", "MX", "BR", "CL"]
    total_amount: float = Field(gt=0.0, description="El monto debe ser estrictamente mayor a 0")
    order_status: Literal["placed", "preparing", "delivered", "cancelled"]
    created_at: str

    @field_validator("created_at")
    @classmethod
    def validate_iso_timestamp(cls, v: str) -> str:
        try:
            # Validamos que sea un timestamp ISO parseable
            parsed = datetime.fromisoformat(v)
            # Regla de cordura: no permitir fechas del futuro ni anteriores a 2020
            if parsed.year < 2020 or parsed.year > 2030:
                raise ValueError("Timestamp fuera de rango histórico razonable.")
            return v
        except Exception as e:
            raise ValueError(f"Formato de fecha inválido: {v}") from e