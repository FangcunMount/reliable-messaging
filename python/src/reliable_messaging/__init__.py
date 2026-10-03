"""Embedded delivery primitives; no constructor performs I/O or starts work."""

from reliable_messaging.delivery import (
    Confirmation,
    DeliveryRejected,
    DeliveryResult,
    Outcome,
    deliver_durable,
)
from reliable_messaging.message import Message
from reliable_messaging.relay import PeriodicRelay

__all__ = [
    "Confirmation",
    "DeliveryRejected",
    "DeliveryResult",
    "Message",
    "Outcome",
    "PeriodicRelay",
    "deliver_durable",
]
