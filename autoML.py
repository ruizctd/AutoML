"""
Renovación automática de publicaciones de autos en MercadoLibre México.

Flujo por publicación:
  1. GET detalle + descripción
  2. POST nueva publicación → si falla, abortar esta publicación
  3. PUT cerrar la vieja → solo si el POST fue exitoso
  4. POST descripción a la nueva → si falla, solo se loggea
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv, set_key

# ---------------------------------------------------------------------------
# Configuración inicial
# ---------------------------------------------------------------------------

load_dotenv()

ENV_FILE = Path(".env")
LOG_FILE_PUBLICACIONES = "publicaciones.log"
LOG_FILE_ERRORES = "errores.log"
ML_BASE = "https://api.mercadolibre.com"

DAYS_TO_RENEW = int(os.getenv("DAYS_TO_RENEW", "30"))
MAX_RENEWALS = int(os.getenv("MAX_RENEWALS", "0"))  # 0 = sin límite
LISTING_TYPE_CASCADE = ["gold_premium", "gold", "silver", "bronze", "free"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# Log de consola (debug en tiempo real)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
log = logging.getLogger("main")
log.setLevel(logging.INFO)
log.addHandler(_console_handler)

# Log de publicaciones: una línea por auto renovado (✅ / ❌)
_pub_handler = logging.FileHandler(LOG_FILE_PUBLICACIONES, encoding="utf-8")
_pub_handler.setFormatter(logging.Formatter("%(message)s"))
pub_log = logging.getLogger("publicaciones")
pub_log.setLevel(logging.INFO)
pub_log.addHandler(_pub_handler)
pub_log.propagate = False

# Log de errores: detalle técnico de cada fallo
_err_handler = logging.FileHandler(LOG_FILE_ERRORES, encoding="utf-8")
_err_handler.setFormatter(logging.Formatter("%(message)s"))
err_log = logging.getLogger("errores")
err_log.setLevel(logging.INFO)
err_log.addHandler(_err_handler)
err_log.propagate = False


# ---------------------------------------------------------------------------
# Manejo de tokens
# ---------------------------------------------------------------------------

def refresh_access_token() -> str:
    """Renueva el access token y persiste el nuevo refresh_token en .env."""
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    refresh_token = os.getenv("REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise EnvironmentError("Faltan CLIENT_ID, CLIENT_SECRET o REFRESH_TOKEN en .env")

    resp = requests.post(
        f"{ML_BASE}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    new_access_token = data["access_token"]
    new_refresh_token = data["refresh_token"]

    # Persistir el nuevo refresh_token para la próxima ejecución
    os.environ["REFRESH_TOKEN"] = new_refresh_token
    set_key(str(ENV_FILE), "REFRESH_TOKEN", new_refresh_token)

    log.info("Token renovado correctamente. Expira en %s segundos.", data.get("expires_in", 21600))
    return new_access_token


# ---------------------------------------------------------------------------
# Helpers de API
# ---------------------------------------------------------------------------

def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def get_all_active_items(token: str, user_id: str) -> list[str]:
    """Devuelve todos los item_ids activos, paginando de 50 en 50."""
    item_ids = []
    offset = 0
    limit = 50

    while True:
        resp = requests.get(
            f"{ML_BASE}/users/{user_id}/items/search",
            params={
                "sort": "stop_time_asc",
                "status": "active",
                "limit": limit,
                "offset": offset,
            },
            headers=auth_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        item_ids.extend(batch)

        total = data.get("paging", {}).get("total", 0)
        offset += limit
        if offset >= total:
            break

    log.info("Total publicaciones activas encontradas: %d", len(item_ids))
    return item_ids


def get_item_detail(token: str, item_id: str) -> dict:
    resp = requests.get(
        f"{ML_BASE}/items/{item_id}",
        headers=auth_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_item_description(token: str, item_id: str) -> str:
    resp = requests.get(
        f"{ML_BASE}/items/{item_id}/description",
        headers=auth_headers(token),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("plain_text", "")


def post_new_item(token: str, body: dict) -> str:
    """Crea la publicación nueva y devuelve el nuevo item_id."""
    resp = requests.post(
        f"{ML_BASE}/items",
        json=body,
        headers={**auth_headers(token), "Content-Type": "application/json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def close_item(token: str, item_id: str) -> None:
    resp = requests.put(
        f"{ML_BASE}/items/{item_id}",
        json={"status": "closed"},
        headers={**auth_headers(token), "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()


def post_description(token: str, item_id: str, plain_text: str) -> None:
    resp = requests.post(
        f"{ML_BASE}/items/{item_id}/description",
        json={"plain_text": plain_text},
        headers={**auth_headers(token), "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Construcción del body para la nueva publicación
# ---------------------------------------------------------------------------

def build_new_item_body(detail: dict) -> dict:
    """
    Construye el body del POST a partir del detalle del item existente.
    Solo incluye los campos que acepta el endpoint de creación.
    """
    body = {
        "title": detail["title"],
        "category_id": detail["category_id"],
        "price": detail["price"],
        "currency_id": detail["currency_id"],
        "available_quantity": detail.get("available_quantity", 1),
        "buying_mode": detail.get("buying_mode", "classified"),
        "condition": detail.get("condition", "used"),
        "listing_type_id": detail.get("listing_type_id", "silver"),
        "channels": detail.get("channels", ["marketplace"]),
    }

    # seller_contact (activa botón WhatsApp si phone2 está presente)
    if detail.get("seller_contact"):
        sc = detail["seller_contact"]
        body["seller_contact"] = {
            "phone": sc.get("phone", ""),
            "country_code": sc.get("country_code", "52"),
            "area_code": sc.get("area_code", ""),
            "phone2": sc.get("phone2", ""),
            "country_code2": sc.get("country_code2", "52"),
            "area_code2": sc.get("area_code2", ""),
        }

    # location
    if detail.get("location"):
        loc = detail["location"]
        location_body = {"country": {"id": loc.get("country", {}).get("id", "MX")}}

        if loc.get("state", {}).get("id"):
            location_body["state"] = {"id": loc["state"]["id"]}
        if loc.get("city", {}).get("id"):
            location_body["city"] = {"id": loc["city"]["id"]}
        if loc.get("neighborhood", {}).get("id"):
            location_body["neighborhood"] = {"id": loc["neighborhood"]["id"]}
        if loc.get("latitude") is not None:
            location_body["latitude"] = loc["latitude"]
        if loc.get("longitude") is not None:
            location_body["longitude"] = loc["longitude"]

        body["location"] = location_body

    # pictures: solo los IDs
    if detail.get("pictures"):
        body["pictures"] = [{"id": p["id"]} for p in detail["pictures"] if p.get("id")]

    # attributes: filtrar solo los que tienen value_id o value_name
    if detail.get("attributes"):
        attrs = []
        for attr in detail["attributes"]:
            entry = {"id": attr["id"]}
            if attr.get("value_id"):
                entry["value_id"] = attr["value_id"]
            if attr.get("value_name"):
                entry["value_name"] = attr["value_name"]
            if len(entry) > 1:  # tiene al menos un valor
                attrs.append(entry)
        if attrs:
            body["attributes"] = attrs

    return body


# ---------------------------------------------------------------------------
# Lógica principal de renovación
# ---------------------------------------------------------------------------

def days_since_published(start_time_str: str) -> float:
    """Calcula días transcurridos desde start_time (formato ISO 8601)."""
    start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    return (now - start_time).total_seconds() / 86400


def renew_item(token: str, item_id: str) -> Optional[dict]:
    """
    Ejecuta el ciclo completo de renovación para un item.
    Devuelve un dict con el resultado o None si no aplicaba renovación.
    """
    log.info("--- Procesando: %s ---", item_id)

    # 1. GET detalle
    try:
        detail = get_item_detail(token, item_id)
    except requests.HTTPError as e:
        log.error("[%s] Error al obtener detalle: %s", item_id, e)
        return {"item_id": item_id, "title": "desconocido", "ok": False, "error": str(e)}

    title = detail.get("title", "sin título")
    start_time = detail.get("start_time", "")
    if not start_time:
        log.warning("[%s] Sin start_time, omitiendo.", item_id)
        return None

    days = days_since_published(start_time)
    log.info("[%s] '%s' — %.1f días publicada.", item_id, title, days)

    if days < DAYS_TO_RENEW:
        log.info("[%s] No cumple %d días aún. Omitiendo.", item_id, DAYS_TO_RENEW)
        return None  # No aplica renovación, no se cuenta

    # 2. GET descripción
    try:
        description = get_item_description(token, item_id)
    except requests.HTTPError as e:
        log.warning("[%s] No se pudo obtener descripción: %s", item_id, e)
        description = ""

    # 3. POST nueva publicación con cascada de listing types
    new_body = build_new_item_body(detail)
    original_type = new_body["listing_type_id"]
    start_index = LISTING_TYPE_CASCADE.index(original_type) if original_type in LISTING_TYPE_CASCADE else 0

    new_item_id = None
    used_type = None
    for listing_type in LISTING_TYPE_CASCADE[start_index:]:
        new_body["listing_type_id"] = listing_type
        try:
            new_item_id = post_new_item(token, new_body)
            used_type = listing_type
            if listing_type != original_type:
                log.warning("[%s] Publicado como '%s' (fallback desde '%s' por cuota llena).",
                            item_id, listing_type, original_type)
            else:
                log.info("[%s] Nueva publicación creada: %s", item_id, new_item_id)
            break
        except requests.HTTPError as e:
            body_text = e.response.text if e.response is not None else "sin respuesta"
            if "not available quota" in body_text.lower():
                log.warning("[%s] Cuota llena para '%s'. Probando siguiente tipo...", item_id, listing_type)
                continue
            else:
                log.error("[%s] FALLO al crear publicación: %s | Respuesta: %s", item_id, e, body_text)
                log.error("[%s] Body enviado:\n%s", item_id, json.dumps(new_body, ensure_ascii=False, indent=2))
                return {"item_id": item_id, "title": title, "days": days, "ok": False, "error": body_text}

    if new_item_id is None:
        log.error("[%s] Todos los listing types agotaron su cuota. No se renovó.", item_id)
        return {"item_id": item_id, "title": title, "days": days, "ok": False,
                "error": "Cuota agotada en todos los tipos disponibles"}

    # 4. PUT cerrar publicación vieja
    try:
        close_item(token, item_id)
        log.info("[%s] Publicación vieja cerrada correctamente.", item_id)
    except requests.HTTPError as e:
        log.error("[%s] Error al cerrar publicación vieja: %s", item_id, e)

    # 5. POST descripción a la nueva (no crítico)
    if description:
        try:
            post_description(token, new_item_id, description)
            log.info("[%s] Descripción agregada a %s.", item_id, new_item_id)
        except requests.HTTPError as e:
            log.warning("[%s] No se pudo agregar descripción a %s: %s", item_id, new_item_id, e)
    else:
        log.info("[%s] Sin descripción para agregar.", item_id)

    log.info("[%s] Renovación completada → nuevo ID: %s", item_id, new_item_id)
    return {"item_id": item_id, "new_item_id": new_item_id, "title": title,
            "days": days, "ok": True, "original_type": original_type, "used_type": used_type}


def log_publicacion_exitosa(fecha: str, r: dict) -> None:
    fallback_tag = ""
    if r.get("used_type") and r["used_type"] != r.get("original_type"):
        fallback_tag = f" [fallback: {r['original_type']} → {r['used_type']}]"
    pub_log.info(
        "%s  ✅  %s | ID viejo: %s → ID nuevo: %s | %.0f días publicado%s",
        fecha, r["title"], r["item_id"], r["new_item_id"], r["days"], fallback_tag,
    )


def log_publicacion_error(fecha: str, r: dict) -> None:
    pub_log.info(
        "%s  ❌  %s | ID: %s | No se pudo renovar",
        fecha, r.get("title", "desconocido"), r["item_id"],
    )
    err_log.info(
        "%s  ❌  %s | ID: %s\n    Error: %s\n",
        fecha, r.get("title", "desconocido"), r["item_id"], r.get("error", "error desconocido"),
    )


def write_cycle_header(fecha: str) -> None:
    limite = f"  |  límite: {MAX_RENEWALS}" if MAX_RENEWALS > 0 else ""
    sep = "─" * 64
    pub_log.info("\n%s\n  CICLO  %s%s\n%s", sep, fecha, limite, sep)
    err_log.info("\n%s\n  CICLO  %s%s\n%s", sep, fecha, limite, sep)


def run_renewal_cycle() -> None:
    """Ciclo completo: refresh token → obtener items → renovar los que aplican."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("========== Iniciando ciclo de renovación ==========")
    write_cycle_header(fecha)

    user_id = os.getenv("USER_ID")
    if not user_id:
        log.error("USER_ID no configurado en .env")
        return

    # Refresh token al inicio de cada ciclo
    try:
        token = refresh_access_token()
    except Exception as e:
        log.error("No se pudo renovar el token: %s", e)
        err_log.info("%s  Error al renovar token: %s\n", fecha, e)
        return

    # Obtener todos los items activos
    try:
        item_ids = get_all_active_items(token, user_id)
    except requests.HTTPError as e:
        log.error("Error al obtener publicaciones activas: %s", e)
        return

    if not item_ids:
        log.info("No hay publicaciones activas.")
        pub_log.info("%s  Sin publicaciones activas.\n", fecha)
        return

    renovadas = 0

    for item_id in item_ids:
        if MAX_RENEWALS > 0 and renovadas >= MAX_RENEWALS:
            log.info("Límite de %d renovaciones alcanzado. Deteniendo ciclo.", MAX_RENEWALS)
            break

        try:
            result = renew_item(token, item_id)
            if result is None:
                pass  # No aplicaba renovación
            elif result["ok"]:
                log_publicacion_exitosa(fecha, result)
                renovadas += 1
            else:
                log_publicacion_error(fecha, result)
        except Exception as e:
            log.error("[%s] Error inesperado: %s", item_id, e)
            log_publicacion_error(fecha, {"item_id": item_id, "title": "desconocido", "error": str(e)})

        time.sleep(1)

    log.info("========== Ciclo finalizado ==========\n")


# ---------------------------------------------------------------------------
# Entry point y scheduler
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    limite_msg = f", máximo {MAX_RENEWALS} por ciclo" if MAX_RENEWALS > 0 else ", sin límite de renovaciones"
    log.info("Script iniciado. Renovando publicaciones con >=%d días%s.", DAYS_TO_RENEW, limite_msg)
    run_renewal_cycle()
