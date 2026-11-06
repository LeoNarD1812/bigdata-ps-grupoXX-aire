
import json
from pathlib import Path
from datetime import datetime, timezone
from jsonschema import Draft202012Validator, FormatChecker

RUTA = Path(__file__).parent / "vuelo_estado_v1.schema.json"

with open(RUTA, encoding="utf-8") as archivo:
    esquema = json.load(archivo)

Draft202012Validator.check_schema(esquema)

validador = Draft202012Validator(
    esquema,
    format_checker=FormatChecker()
)

evento = {
    "eventId": "evt-prueba-001",
    "eventType": "vuelo.estado_actualizado",
    "schemaVersion": 1,
    "flightId": "VUELO-DEMO-001",
    "airportCode": "SPJC",
    "status": "EN_VUELO",
    "eventTime": datetime.now(timezone.utc).isoformat(),
    "source": "simulacion"
}

errores = list(validador.iter_errors(evento))

if not errores:
    print("EVENTO VALIDO: cumple el contrato V1")
else:
    for error in errores:
        print("EVENTO INVALIDO:", error.message)

# Prueba negativa: estado no permitido
evento["status"] = "ESTADO_DESCONOCIDO"

errores = list(validador.iter_errors(evento))

if errores:
    print("PRUEBA NEGATIVA CORRECTA: evento rechazado")
    for error in errores:
        print("Motivo:", error.message)
else:
    print("ERROR: se aceptó un estado inválido")
