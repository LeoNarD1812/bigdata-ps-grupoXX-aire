
import json
import time
import uuid
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "vuelos-operaciones"

producer = Producer({
    "bootstrap.servers": "localhost:49092",
    "client.id": "producer-operaciones-aereas",
    "acks": "all"
})

def crear_evento():
    vuelo = random.choice([
        ("LPE2258", "SPJC"),
        ("LPE2013", "SPJC"),
        ("VUELO-DEMO-003", "SPZO")
    ])

    return {
        "eventId": str(uuid.uuid4()),
        "eventType": "vuelo.estado_actualizado",
        "schemaVersion": 1,
        "flightId": vuelo[0],
        "airportCode": vuelo[1],
        "status": random.choice([
            "PROGRAMADO", "EN_VUELO", "ATERRIZADO"
        ]),
        "eventTime": datetime.now(
            timezone.utc
        ).isoformat(),
        "source": "simulacion"
    }

def confirmar_envio(error, mensaje):
    if error:
        print(f"ERROR: {error}")
    else:
        print(
            f"ENVIADO | topic={mensaje.topic()} "
            f"partition={mensaje.partition()} "
            f"offset={mensaje.offset()}"
        )

try:
    while True:
        evento = crear_evento()

        producer.produce(
            topic=TOPIC,
            key=evento["flightId"].encode("utf-8"),
            value=json.dumps(evento).encode("utf-8"),
            callback=confirmar_envio
        )

        producer.poll(0)
        print(json.dumps(evento, ensure_ascii=False))
        time.sleep(3)

except KeyboardInterrupt:
    print("Productor detenido")

finally:
    producer.flush()
