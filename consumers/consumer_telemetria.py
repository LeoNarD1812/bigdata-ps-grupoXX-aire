
import json
from pathlib import Path

from confluent_kafka import Consumer
from jsonschema import Draft202012Validator, FormatChecker

TOPIC = "aeronaves-telemetria"

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = (
    BASE_DIR / "contracts" /
    "aeronave_telemetria_v1.schema.json"
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
    "group.id": "grupo-telemetria-s7-validado-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC])

print("CONSUMIDOR DE TELEMETRIA S7")
print("Validacion JSON Schema V1 activada\n")

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

            errores = sorted(
                validador.iter_errors(evento),
                key=lambda error: str(error.path)
            )

            if errores:
                print("\nEVENTO TELEMETRIA RECHAZADO")
                print("Particion:", mensaje.partition())
                print("Offset:", mensaje.offset())

                for error in errores:
                    print("Motivo:", error.message)

            else:
                print("\nTELEMETRIA VALIDA")
                print("ICAO24:", evento["icao24"])
                print("Callsign:", evento.get("callsign"))
                print("Latitud:", evento["latitude"])
                print("Longitud:", evento["longitude"])
                print("Altitud:", evento.get("baroAltitude"))
                print("Velocidad:", evento.get("velocity"))
                print("Velocidad vertical:", evento.get("verticalRate"))
                print("Fecha:", evento["eventTime"])
                print("Particion:", mensaje.partition())
                print("Offset:", mensaje.offset())

        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            print("\nJSON INVALIDO:", error)

        # Confirmar mensajes procesados o clasificados
        # como invalidos en este laboratorio.
        consumer.commit(
            message=mensaje,
            asynchronous=False
        )

except KeyboardInterrupt:
    print("\nConsumidor detenido")

finally:
    consumer.close()
