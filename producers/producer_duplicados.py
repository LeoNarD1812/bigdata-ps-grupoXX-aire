
import json
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:49092",
    "acks": "all"
})

fecha = datetime.now(timezone.utc).isoformat()

eventos = [
    ("evento-dedup-001", "abcdef"),
    ("evento-dedup-001", "abcdef"),
    ("evento-dedup-001", "abcdef"),
    ("evento-dedup-002", "123abc")
]

for event_id, icao24 in eventos:
    evento = {
        "eventId": event_id,
        "icao24": icao24,
        "eventTime": fecha,
        "source": "prueba_s8"
    }

    producer.produce(
        "s8-dedup-prueba",
        key=icao24,
        value=json.dumps(evento)
    )

    print("PUBLICADO:", event_id)

pendientes = producer.flush(10)

if pendientes:
    raise RuntimeError("Mensajes pendientes de envio")

print("PRUEBA FINALIZADA")
