
import json
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from jsonschema import Draft202012Validator, FormatChecker

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "aeronautica/sensores/lecturas"

KAFKA_TOPIC = "sensores-aereos-telemetria"

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = (
    BASE_DIR / "contracts" / "sensor_aereo_v1.schema.json"
)

with open(SCHEMA_PATH, encoding="utf-8") as archivo:
    schema = json.load(archivo)

Draft202012Validator.check_schema(schema)

validador = Draft202012Validator(
    schema,
    format_checker=FormatChecker()
)

producer = Producer({
    "bootstrap.servers": "localhost:49092",
    "client.id": "puente-mqtt-kafka-s7",
    "acks": "all"
})


def confirmar_kafka(error, mensaje):
    if error:
        print("ERROR KAFKA:", error)
    else:
        print(
            f"KAFKA CONFIRMADO | "
            f"particion={mensaje.partition()} "
            f"offset={mensaje.offset()}"
        )


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("CONECTADO A MOSQUITTO")
        client.subscribe(MQTT_TOPIC, qos=1)
        print("SUSCRITO:", MQTT_TOPIC)
    else:
        print("ERROR MQTT:", reason_code)


def on_message(client, userdata, mensaje):
    try:
        evento = json.loads(mensaje.payload.decode("utf-8"))

        errores = list(validador.iter_errors(evento))

        if errores:
            print("\nEVENTO MQTT RECHAZADO")

            for error in errores:
                print("Motivo:", error.message)

            return

        device_id = evento["deviceId"]

        print(
            f"\nMQTT RECIBIDO | {device_id}"
            f" | secuencia={evento['sequence']}"
        )

        producer.produce(
            KAFKA_TOPIC,
            key=device_id.encode("utf-8"),
            value=json.dumps(evento).encode("utf-8"),
            on_delivery=confirmar_kafka
        )

        producer.poll(0)

    except (
        UnicodeDecodeError,
        json.JSONDecodeError
    ) as error:
        print("JSON MQTT INVALIDO:", error)

    except BufferError as error:
        print("BUFFER KAFKA LLENO:", error)


cliente = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="puente-mqtt-kafka-s7"
)

cliente.on_connect = on_connect
cliente.on_message = on_message

try:
    print("INICIANDO PUENTE MQTT → KAFKA")
    cliente.connect(MQTT_BROKER, MQTT_PORT, 60)
    cliente.loop_forever()

except KeyboardInterrupt:
    print("\nPuente detenido")

finally:
    cliente.disconnect()
    producer.flush(10)
