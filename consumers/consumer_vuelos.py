
import json
from pathlib import Path

from confluent_kafka import Consumer
from jsonschema import Draft202012Validator, FormatChecker

TOPIC = "vuelos-operaciones"

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = (
    BASE_DIR / "contracts" / "vuelo_estado_v1.schema.json"
)

with open(SCHEMA_PATH, encoding="utf-8") as archivo:
    esquema = json.load(archivo)

Draft202012Validator.check_schema(esquema)

validador = Draft202012Validator(
    esquema,
    format_checker=FormatChecker()
)

consumer = Consumer({
    "bootstrap.servers": "localhost:49092",
    "group.id": "grupo-operaciones-validador-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC])

print("CONSUMIDOR AEREO INICIADO")
print("Validacion JSON Schema V1 activa")
print("Esperando eventos...\n")

try:
    while True:
        mensaje = consumer.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            print("ERROR KAFKA:", mensaje.error())
            continue

        try:
            evento = json.loads(
                mensaje.value().decode("utf-8")
            )

            errores = list(
                validador.iter_errors(evento)
            )

            if errores:
                print("\nEVENTO RECHAZADO")
                print("Particion:", mensaje.partition())
                print("Offset:", mensaje.offset())

                for error in errores:
                    print("Motivo:", error.message)

            else:
                print("\nEVENTO VALIDO")
                print("Vuelo:", evento["flightId"])
                print("Estado:", evento["status"])
                print("Aeropuerto:", evento["airportCode"])
                print("Fuente:", evento["source"])
                print("Particion:", mensaje.partition())
                print("Offset:", mensaje.offset())

        except (
            UnicodeDecodeError,
            json.JSONDecodeError
        ) as error:
            print("\nJSON INVALIDO:", error)

        except Exception as error:
            print("\nERROR DE PROCESAMIENTO:", error)
            continue

        # Confirmamos el offset tras procesar
        # o clasificar el mensaje como invalido.
        consumer.commit(message=mensaje, asynchronous=False)

except KeyboardInterrupt:
    print("\nConsumidor detenido")

finally:
    consumer.close()
