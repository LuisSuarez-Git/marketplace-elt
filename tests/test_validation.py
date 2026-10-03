import pytest
from pydantic import ValidationError
from src.ingestion.models import OrderEvent


def test_valid_order_event():
    """Valida que una orden correcta pase sin problemas."""
    payload = {
        "order_id": "ord-12345",
        "user_id": 1050,
        "country_code": "CO",
        "total_amount": 45.50,
        "order_status": "delivered",
        "created_at": "2026-10-01T15:30:00+00:00",
    }
    order = OrderEvent(**payload)
    assert order.total_amount == 45.50
    assert order.country_code == "CO"


def test_negative_total_amount_raises_error():
    """Simula una API mandando montos negativos."""
    payload = {
        "order_id": "ord-999",
        "user_id": 1050,
        "country_code": "CO",
        "total_amount": -10.0,  # <-- CORRUPCIÓN DE NEGOCIO
        "order_status": "delivered",
        "created_at": "2026-10-01T15:30:00+00:00",
    }
    with pytest.raises(ValidationError):
        OrderEvent(**payload)


def test_invalid_country_code_raises_error():
    """Simula una API mandando un país no soportado."""
    payload = {
        "order_id": "ord-999",
        "user_id": 1050,
        "country_code": "US",  # <-- PAÍS NO PERMITIDO EN ESTE DOMINIO
        "total_amount": 25.0,
        "order_status": "delivered",
        "created_at": "2026-10-01T15:30:00+00:00",
    }
    with pytest.raises(ValidationError):
        OrderEvent(**payload)