
import os
import sys
import requests

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

API_URL = "https://opensky-network.org/api/states/all"

client_id = os.getenv("OPENSKY_CLIENT_ID")
client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

if not client_id or not client_secret:
    print("ERROR: Faltan las credenciales de GitHub Secrets.")
    sys.exit(1)

try:
    # 1. Solicitar token OAuth2
    auth_response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )

    print("OAuth HTTP:", auth_response.status_code)

    if auth_response.status_code != 200:
        print("ERROR: OpenSky no aceptó la autenticación.")
        sys.exit(1)

    token = auth_response.json().get("access_token")

    if not token:
        print("ERROR: No se recibió el token.")
        sys.exit(1)

    # 2. Consultar el estado mundial de aeronaves
    api_response = requests.get(
        API_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )

    print("API HTTP:", api_response.status_code)

    if api_response.status_code != 200:
        print("ERROR: No se pudo obtener el estado mundial.")
        sys.exit(1)

    data = api_response.json()
    aircraft = data.get("states") or []

    print("Aeronaves recibidas:", len(aircraft))
    print("Timestamp OpenSky:", data.get("time"))
    print(
        "Créditos restantes:",
        api_response.headers.get("X-Rate-Limit-Remaining", "No informado")
    )

    print("PRUEBA COMPLETADA CORRECTAMENTE")

except requests.RequestException as error:
    print("ERROR DE CONEXIÓN:", type(error).__name__)
    sys.exit(1)

except (ValueError, KeyError):
    print("ERROR: Respuesta inesperada de OpenSky.")
    sys.exit(1)
