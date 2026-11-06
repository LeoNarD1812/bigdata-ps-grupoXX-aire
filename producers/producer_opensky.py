
import json
import os
import time
import uuid
import random
from collections import OrderedDict
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer, KafkaException


# ==========================================
# CONFIGURACION
# ==========================================

API_URL = "https://opensky-network.org/api/states/all"
TOPIC = "aeronaves-telemetria"

REGION = {
    "lamin": -17,
    "lomin": -78,
    "lamax": -10,
    "lomax": -71
}

KAFKA_SERVER = "localhost:49092"

INTERVALO = 120
ESPERA_ERROR = 60
ESPERA_CUOTA = 3600

# Reservar algunos creditos para otras pruebas
CREDITOS_MINIMOS = 10

# Aproximacion para esta region: 49 grados cuadrados
COSTO_CONSULTA = 2

# Limite de memoria para identidades recientes
MAX_IDENTIDADES = 50000

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "client.id": "productor-opensky",
    "acks": "all",
    "enable.idempotence": True
})

sesion = requests.Session()

# Las observaciones procesadas quedan en memoria
# durante la ejecucion del productor.
observaciones_publicadas = OrderedDict()

consultas = 0
total_publicados = 0


# ==========================================
# FECHAS
# ==========================================

def fecha_utc(timestamp):
    return datetime.fromtimestamp(
        timestamp, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")


# ==========================================
# NORMALIZACION
# ==========================================

def normalizar(state, api_time):

    if len(state) < 17:
        return None

    icao24 = state[0]
    posicion_tiempo = state[3]
    longitud = state[5]
    latitud = state[6]

    if not icao24 or posicion_tiempo is None:
        return None

    if latitud is None or longitud is None:
        return None

    if not (-90 <= latitud <= 90):
        return None

    if not (-180 <= longitud <= 180):
        return None

    if not (0 <= api_time - posicion_tiempo <= 120):
        return None

    altitud = state[7]
    velocidad = state[9]
    velocidad_vertical = state[11]

    if altitud is not None:
        if not (-500 <= altitud <= 20000):
            return None

    if velocidad is not None:
        if not (0 <= velocidad <= 400):
            return None

    if velocidad_vertical is not None:
        if not (-100 <= velocidad_vertical <= 100):
            return None

    return {
        "eventId": str(uuid.uuid4()),
        "eventType": "aeronave.telemetria",
        "schemaVersion": 1,
        "icao24": str(icao24).lower(),
        "callsign": (
            state[1].strip() if state[1] else None
        ),
        "originCountry": state[2],
        "timePosition": posicion_tiempo,
        "eventTime": fecha_utc(posicion_tiempo),
        "latitude": latitud,
        "longitude": longitud,
        "baroAltitude": altitud,
        "onGround": state[8],
        "velocity": velocidad,
        "trueTrack": state[10],
        "verticalRate": velocidad_vertical,
        "geoAltitude": state[13],
        "positionSource": state[16],
        "observedAt": fecha_utc(api_time),
        "source": "opensky"
    }


# ==========================================
# CONFIRMACION KAFKA
# ==========================================

def confirmar(error, mensaje):

    if error:
        print("ERROR KAFKA:", error, flush=True)
        return

    print(
        f"ENVIADO | "
        f"particion={mensaje.partition()} "
        f"offset={mensaje.offset()}",
        flush=True
    )


# ==========================================
# LIMPIAR IDENTIDADES ANTIGUAS
# ==========================================

def recordar_identidad(identidad):

    observaciones_publicadas[identidad] = True
    observaciones_publicadas.move_to_end(identidad)

    while len(observaciones_publicadas) > MAX_IDENTIDADES:
        observaciones_publicadas.popitem(last=False)


# ==========================================
# ESPERA SEGURA
# ==========================================

def esperar(segundos, motivo):

    segundos = max(1, int(segundos))

    print(
        f"{motivo}. Siguiente intento en "
        f"{segundos} segundos.",
        flush=True
    )

    time.sleep(segundos)


# ==========================================
# PUBLICAR EVENTOS
# ==========================================

def publicar_observaciones(aeronaves, api_time):

    validas = 0
    enviadas = 0
    descartadas = 0

    for state in aeronaves:

        evento = normalizar(state, api_time)

        if evento is None:
            descartadas += 1
            continue

        validas += 1

        identidad = (
            evento["icao24"],
            evento["timePosition"]
        )

        if identidad in observaciones_publicadas:
            continue

        try:
            producer.produce(
                TOPIC,
                key=evento["icao24"].encode("utf-8"),
                value=json.dumps(
                    evento,
                    ensure_ascii=False
                ).encode("utf-8"),
                callback=confirmar
            )

        except BufferError:
            producer.poll(5)

            producer.produce(
                TOPIC,
                key=evento["icao24"].encode("utf-8"),
                value=json.dumps(evento).encode("utf-8"),
                callback=confirmar
            )

        # Evita duplicados dentro de esta ejecucion.
        # La confirmacion efectiva se observa en callback.
        recordar_identidad(identidad)

        enviadas += 1
        producer.poll(0)

    pendientes = producer.flush(30)

    if pendientes:
        print(
            "ADVERTENCIA: mensajes sin confirmar:",
            pendientes,
            flush=True
        )

    print("\nRESUMEN CONSULTA")
    print("Observaciones recibidas:", len(aeronaves))
    print("Observaciones validas:", validas)
    print("Observaciones descartadas:", descartadas)
    print("Eventos puestos en cola:", enviadas)
    print("Total acumulado en cola:", total_publicados + enviadas)
    print("Mensajes aun pendientes:", pendientes)

    return enviadas


# ==========================================
# CONSULTAR API CONTINUAMENTE
# ==========================================

print("=" * 55)
print("PRODUCTOR OPENSKY CONTINUO INICIADO")
print("Region:", REGION)
print("Kafka:", KAFKA_SERVER)
print("Topic:", TOPIC)
print("Intervalo:", INTERVALO, "segundos")
print("Detener con CTRL + C")
print("=" * 55, flush=True)


try:

    while True:

        consultas += 1

        print(
            f"\nCONSULTA OPENSKY {consultas}",
            flush=True
        )

        try:

            respuesta = sesion.get(
                API_URL,
                params=REGION,
                timeout=30
            )

            codigo = respuesta.status_code

            creditos_texto = respuesta.headers.get(
                "X-Rate-Limit-Remaining"
            )

            try:
                creditos = (
                    int(creditos_texto)
                    if creditos_texto is not None
                    else None
                )
            except ValueError:
                creditos = None

            print("HTTP:", codigo)
            print(
                "Creditos restantes:",
                creditos if creditos is not None
                else "No indicado"
            )

            # ----------------------------------
            # LIMITE DE PETICIONES
            # ----------------------------------

            if codigo == 429:

                retry_header = respuesta.headers.get(
                    "X-Rate-Limit-Retry-After-Seconds"
                )

                try:
                    espera = max(
                        60,
                        int(float(retry_header))
                    )
                except (ValueError, TypeError):
                    espera = ESPERA_CUOTA

                esperar(
                    espera,
                    "Cuota OpenSky agotada"
                )
                continue

            # ----------------------------------
            # ACCESO RESTRINGIDO
            # ----------------------------------

            if codigo in (401, 403):

                print(
                    "ERROR: acceso restringido "
                    "por OpenSky.",
                    flush=True
                )

                esperar(
                    ESPERA_CUOTA,
                    "Acceso no disponible"
                )
                continue

            # ----------------------------------
            # ERRORES TEMPORALES
            # ----------------------------------

            if codigo >= 500:

                esperar(
                    ESPERA_ERROR,
                    "Servidor OpenSky no disponible"
                )
                continue

            respuesta.raise_for_status()

            datos = respuesta.json()

            api_time = datos["time"]
            aeronaves = datos.get("states") or []

            nuevas = publicar_observaciones(
                aeronaves,
                api_time
            )

            total_publicados += nuevas

            print(
                "\nCONSULTA COMPLETADA",
                flush=True
            )

            # ----------------------------------
            # CONTROL DE CREDITOS
            # ----------------------------------

            if creditos is not None:

                if creditos < (
                    CREDITOS_MINIMOS + COSTO_CONSULTA
                ):

                    print(
                        "Reserva de creditos alcanzada.",
                        flush=True
                    )

                    esperar(
                        ESPERA_CUOTA,
                        "Pausa para proteger la cuota"
                    )

                    continue

            esperar(
                INTERVALO,
                "Consulta completada"
            )

        except requests.RequestException as error:

            print("ERROR DE RED:", error, flush=True)

            esperar(
                ESPERA_ERROR,
                "Reintentando conexion"
            )

        except (ValueError, KeyError, TypeError) as error:

            print(
                "ERROR EN RESPUESTA:",
                error,
                flush=True
            )

            esperar(
                ESPERA_ERROR,
                "Respuesta invalida"
            )

        except KafkaException as error:

            print("ERROR KAFKA:", error, flush=True)

            esperar(
                ESPERA_ERROR,
                "Reintentando Kafka"
            )

except KeyboardInterrupt:

    print(
        "\nDETENCION MANUAL DEL PRODUCTOR",
        flush=True
    )

finally:

    print("Cerrando productor...", flush=True)

    pendientes = producer.flush(30)

    if pendientes:
        print(
            "ADVERTENCIA: mensajes pendientes:",
            pendientes,
            flush=True
        )

    sesion.close()

    print(
        "PRODUCTOR OPENSKY FINALIZADO",
        flush=True
    )
