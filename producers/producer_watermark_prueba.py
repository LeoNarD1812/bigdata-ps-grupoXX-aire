
import json
import time
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:49092",
    "acks": "all"
})

TOPIC = "s8-watermark-prueba"

# Una hora de referencia, redondeada al minuto.
base = datetime.now(timezone.utc).replace(
    second=0, microsecond=0
) - timedelta(minutes=15)


def publicar(nombre, minutos, segundos=10):
    fecha = base + timedelta(
        minutes=minutos,
        seconds=segundos
    )

    evento = {
        "eventId": nombre,
        "icao24": "abcdef",
        "eventTime": fecha.isoformat().replace(
            "+00:00", "Z"
        ),
        "source": "prueba_s8"
    }

    producer.produce(
        TOPIC,
        key=b"abcdef",
        value=json.dumps(evento).encode("utf-8")
    )

    pendientes = producer.flush(10)

    if pendientes:
        raise RuntimeError(
            "Quedaron mensajes pendientes de envio"
        )

    print(
        f"PUBLICADO: {nombre} | "
        f"Tiempo evento: {evento['eventTime']}"
    )


print("PRUEBA CONTROLADA DE WATERMARK")

# Primer evento: minuto 0
publicar("evento-inicial", 0)
time.sleep(12)

# Segundo evento: minuto 1
publicar("evento-segundo", 1)
time.sleep(12)

# Tercer evento: minuto 5
# Avanza el watermark más allá de las primeras ventanas.
publicar("evento-avance-watermark", 5)
time.sleep(12)

# Evento antiguo que llega después del avance.
publicar("evento-demasiado-tardio", 0, 40)
time.sleep(12)

# Otro evento reciente para continuar los micro-lotes.
publicar("evento-final", 7)

print("PRUEBA DE PUBLICACION TERMINADA")
