
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER = "localhost"
PUERTO = 1883
TOPIC = "aeronautica/sensores/lecturas"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--invalid",
    action="store_true",
    help="Publicar una lectura fuera de rango"
)
args = parser.parse_args()

sensores = {
    "sensor-aereo-001": {
        "altitude": 1500.0,
        "velocity": 120.0,
        "verticalRate": 3.0
    },
    "sensor-aereo-002": {
        "altitude": 3500.0,
        "velocity": 180.0,
        "verticalRate": -2.0
    }
}

cliente = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="simulador-aereo-s7"
)

cliente.connect(BROKER, PUERTO, keepalive=60)
cliente.loop_start()

try:
    for secuencia in range(1, 11):
        for device_id, estado in sensores.items():

            estado["verticalRate"] = round(
                max(-10, min(
                    10,
                    estado["verticalRate"] +
                    random.uniform(-0.5, 0.5)
                )), 2
            )

            estado["altitude"] = round(
                max(0, estado["altitude"] +
                    estado["verticalRate"] * 2),
                2
            )

            estado["velocity"] = round(
                max(0, estado["velocity"] +
                    random.uniform(-2, 2)),
                2
            )

            velocidad = estado["velocity"]

            if args.invalid and secuencia == 3:
                if device_id == "sensor-aereo-001":
                    velocidad = 9999.0

            evento = {
                "eventId": str(uuid.uuid4()),
                "eventType": "sensor.aereo.lectura",
                "schemaVersion": 1,
                "deviceId": device_id,
                "sequence": secuencia,
                "eventTime": datetime.now(
                    timezone.utc
                ).isoformat(),
                "altitude": estado["altitude"],
                "velocity": velocidad,
                "verticalRate": estado["verticalRate"],
                "source": "simulacion_mqtt"
            }

            resultado = cliente.publish(
                TOPIC,
                json.dumps(evento),
                qos=1
            )
            resultado.wait_for_publish()

            if resultado.rc == mqtt.MQTT_ERR_SUCCESS:
                print(
                    f"MQTT ENVIADO | {device_id}"
                    f" | secuencia={secuencia}"
                    f" | velocidad={velocidad}"
                )
            else:
                print("ERROR PUBLICACION MQTT:", resultado.rc)

        time.sleep(2)

except KeyboardInterrupt:
    print("Simulador detenido")

finally:
    cliente.disconnect()
    cliente.loop_stop()
