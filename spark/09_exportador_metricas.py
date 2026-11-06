
import json
import time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

RUTA_SPARK = Path(
    "/workspace/artifacts/s9/spark_progress.json"
)

RUTA_AERONAVES = Path(
    "/workspace/artifacts/s9/indicadores_aereos.json"
)


def leer_json(ruta):
    try:
        if ruta.exists():
            return json.loads(
                ruta.read_text(encoding="utf-8")
            )
    except (OSError, ValueError) as error:
        print("Error leyendo", ruta, error, flush=True)

    return {}


def numero(valor, defecto=0):
    try:
        return float(valor) if valor is not None else defecto
    except (TypeError, ValueError):
        return defecto


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path != "/metrics":
            self.send_error(404)
            return

        spark = leer_json(RUTA_SPARK)
        aeronaves = leer_json(RUTA_AERONAVES)

        ahora = time.time()

        spark_reciente = int(
            bool(spark)
            and 0 <= ahora - numero(
                spark.get("updatedAt"), 0
            ) < 45
        )

        aeronaves_recientes = int(
            bool(aeronaves)
            and 0 <= ahora - numero(
                aeronaves.get("updatedAt"), 0
            ) < 90
        )

        metricas = {
            "aire_spark_batch_id": numero(
                spark.get("batchId"), -1
            ),
            "aire_spark_input_rows": numero(
                spark.get("numInputRows")
            ),
            "aire_spark_processed_rows_per_second": numero(
                spark.get("processedRowsPerSecond")
            ),
            "aire_spark_trigger_duration_ms": numero(
                spark.get("triggerExecutionMs")
            ),
            "aire_spark_progress_recent": spark_reciente,

            "aire_aeronaves_observaciones_total": numero(
                aeronaves.get("totalObservaciones")
            ),
            "aire_aeronaves_unicas": numero(
                aeronaves.get("aeronavesUnicas")
            ),
            "aire_aeronaves_altitud_promedio_metros": numero(
                aeronaves.get("altitudPromedio")
            ),
            "aire_aeronaves_velocidad_promedio_mps": numero(
                aeronaves.get("velocidadPromedio")
            ),
            "aire_aeronaves_indicadores_recientes": aeronaves_recientes
        }

        lineas = []

        for nombre, valor in metricas.items():
            lineas.append(
                f"# TYPE {nombre} gauge"
            )
            lineas.append(
                f"{nombre} {valor}"
            )

        contenido = (
            "\n".join(lineas) + "\n"
        ).encode("utf-8")

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/plain; version=0.0.4; charset=utf-8"
        )
        self.send_header(
            "Content-Length",
            str(len(contenido))
        )
        self.end_headers()
        self.wfile.write(contenido)


if __name__ == "__main__":
    servidor = HTTPServer(
        ("0.0.0.0", 8000),
        MetricsHandler
    )

    print(
        "EXPORTER SPARK + AERONAVES INICIADO",
        flush=True
    )

    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("Exporter detenido")
    finally:
        servidor.server_close()
