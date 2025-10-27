# crud.py
import os
import json
from flask import Flask, request, jsonify, abort
import requests
from urllib.parse import urlencode

# =========================
# Config
# =========================
BACKENDLESS_BASE_URL = os.getenv(
    "BACKENDLESS_BASE_URL",
    "https://neatpartner-us.backendless.app/api/data/UsersDB"
)

# Opcional: si tu app requiere claves (muchas tablas públicas no lo piden)
APP_ID = os.getenv("BACKENDLESS_APP_ID")           # p.ej. "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
API_KEY = os.getenv("BACKENDLESS_API_KEY")         # p.ej. "YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY"

DEFAULT_HEADERS = {
    "Content-Type": "application/json"
}
# Agrega headers solo si existen (evita fallar con tablas públicas)
if APP_ID and API_KEY:
    DEFAULT_HEADERS.update({
        "X-Backendless-Application-Id": APP_ID,
        "X-Backendless-API-Key": API_KEY
    })

app = Flask(__name__)

# =========================
# Utilidades
# =========================
def be_error(resp):
    """Normaliza errores de Backendless en respuestas Flask."""
    try:
        payload = resp.json()
    except Exception:
        payload = {"message": resp.text or "Backendless error"}
    return jsonify({"error": True, "status": resp.status_code, "backendless": payload}), resp.status_code

def pick_user_fields(data: dict) -> dict:
    """
    Filtra/normaliza los campos del modelo UsersDB según tu tabla:
      email (str), First Name (str), Last Name (str),
      ID (int), password (str), username (str)
    Campos especiales de Backendless (objectId, created, updated, ownerId) se manejan automáticamente.
    """
    allowed = {
        "email",
        "First Name",
        "Last Name",
        "ID",
        "password",
        "username"
    }
    out = {}
    for k in allowed:
        if k in data:
            out[k] = data[k]
    return out

# =========================
# CRUD Endpoints
# =========================

@app.get("/users")
def list_users():
    """
    Lista usuarios con soporte de paginación y filtros simples.
    Query params opcionales:
      - pageSize (int, default 50)
      - offset (int, default 0)
      - where (Backendless where clause), p.ej.: where=username='leo'
      - sortBy (p.ej. 'created desc')
    """
    page_size = int(request.args.get("pageSize", 50))
    offset = int(request.args.get("offset", 0))
    where = request.args.get("where")
    sort_by = request.args.get("sortBy")

    params = {
        "pageSize": page_size,
        "offset": offset,
    }
    if where:
        params["where"] = where
    if sort_by:
        params["sortBy"] = sort_by

    url = f"{BACKENDLESS_BASE_URL}?{urlencode(params)}"
    resp = requests.get(url, headers=DEFAULT_HEADERS)
    if not resp.ok:
        return be_error(resp)
    # Backendless devuelve lista y, si pides count, también totalObjects.
    return jsonify(resp.json()), 200


@app.get("/users/<object_id>")
def get_user(object_id):
    """Obtiene un usuario por objectId."""
    url = f"{BACKENDLESS_BASE_URL}/{object_id}"
    resp = requests.get(url, headers=DEFAULT_HEADERS)
    if not resp.ok:
        return be_error(resp)
    return jsonify(resp.json()), 200


@app.post("/users")
def create_user():
    """
    Crea un usuario.
    Body (JSON) campos permitidos:
      email, First Name, Last Name, ID, password, username
    """
    try:
        incoming = request.get_json(force=True) or {}
    except Exception:
        abort(400, description="Invalid JSON body")

    payload = pick_user_fields(incoming)

    # Validaciones mínimas de ejemplo (ajusta a tus reglas)
    if "email" not in payload or "username" not in payload:
        abort(400, description="Fields 'email' and 'username' are required")

    url = BACKENDLESS_BASE_URL
    resp = requests.post(url, headers=DEFAULT_HEADERS, data=json.dumps(payload))
    if not resp.ok:
        return be_error(resp)
    return jsonify(resp.json()), 201


@app.put("/users/<object_id>")
@app.patch("/users/<object_id>")
def update_user(object_id):
    """
    Actualiza un usuario por objectId (PUT/PATCH).
    Body (JSON) con cualquiera de:
      email, First Name, Last Name, ID, password, username
    """
    try:
        incoming = request.get_json(force=True) or {}
    except Exception:
        abort(400, description="Invalid JSON body")

    payload = pick_user_fields(incoming)
    if not payload:
        abort(400, description="No updatable fields provided")

    url = f"{BACKENDLESS_BASE_URL}/{object_id}"
    # Backendless soporta PUT para actualizar campos
    resp = requests.put(url, headers=DEFAULT_HEADERS, data=json.dumps(payload))
    if not resp.ok:
        return be_error(resp)
    return jsonify(resp.json()), 200


@app.delete("/users/<object_id>")
def delete_user(object_id):
    """Elimina un usuario por objectId."""
    url = f"{BACKENDLESS_BASE_URL}/{object_id}"
    resp = requests.delete(url, headers=DEFAULT_HEADERS)
    if resp.status_code not in (200, 204):
        return be_error(resp)
    # Algunos planes devuelven `1`/`0`; normalizamos
    return jsonify({"deleted": True, "objectId": object_id}), 200


# =========================
# Helpers de búsqueda comunes
# =========================

@app.get("/users/by-email/<email>")
def find_by_email(email):
    """Búsqueda exacta por email (where)."""
    where = f"email='{email}'"
    url = f"{BACKENDLESS_BASE_URL}?{urlencode({'where': where, 'pageSize': 50, 'offset': 0})}"
    resp = requests.get(url, headers=DEFAULT_HEADERS)
    if not resp.ok:
        return be_error(resp)
    items = resp.json()
    return jsonify(items), 200


@app.get("/users/by-username/<username>")
def find_by_username(username):
    """Búsqueda exacta por username (where)."""
    where = f"username='{username}'"
    url = f"{BACKENDLESS_BASE_URL}?{urlencode({'where': where, 'pageSize': 50, 'offset': 0})}"
    resp = requests.get(url, headers=DEFAULT_HEADERS)
    if not resp.ok:
        return be_error(resp)
    items = resp.json()
    return jsonify(items), 200


# =========================
# Arranque
# =========================
if __name__ == "__main__":
    # FLASK_RUN_PORT o PORT pueden cambiar el puerto (útil para Render/Heroku)
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=True)
