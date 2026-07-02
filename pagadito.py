"""
pagadito.py — Cliente para Pagadito Connect API v2 (Pago Único)
LicitacionLab — app.licitacionlab.com

Variables de entorno requeridas (Railway):
    PAGADITO_UID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    PAGADITO_WSK=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    PAGADITO_SANDBOX=true   # "false" en producción

Uso:
    from pagadito import PagaditoClient
    pg = PagaditoClient()
    resp = pg.exec_trans(ern="LL-abc123-1718000000", amount=29.00,
                         details=[{"quantity": 1, "description": "Plan Mensual LicitacionLab", "price": 29.00}])
    # resp["data"]["url"]   -> redirigir al usuario aquí
    # resp["data"]["token"] -> guardar en tabla pagos

    status = pg.get_status(token=resp["data"]["token"])
    # status["data"]["status"] -> COMPLETED / FAILED / CANCELED / EXPIRED / VERIFYING ...
"""

import os
import httpx

SANDBOX_URL = "https://sandbox-connect.pagadito.com/api/v2"
PRODUCTION_URL = "https://connect.pagadito.com/api/v2"

# Códigos de éxito según documentación Pagadito Connect
CODE_TRANS_REGISTERED = "PG1002"   # exec-trans OK
CODE_TRANS_STATUS = "PG1003"       # get-status OK
CODE_ERN_DUPLICADO = "PG3018"      # "You have already sent this ERN"

# Estados de transacción
ESTADOS_FINALES_OK = {"COMPLETED"}
ESTADOS_FINALES_FALLO = {"FAILED", "CANCELED", "EXPIRED", "REVOKED", "UNCOLLECTABLE"}
ESTADOS_EN_PROCESO = {"REGISTERED", "VERIFYING", "PENDING"}


class PagaditoError(Exception):
    """Error devuelto por la API de Pagadito (código PG2xxx / PG3xxx)."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class PagaditoClient:
    def __init__(self, uid: str | None = None, wsk: str | None = None,
                 sandbox: bool | None = None, timeout: float = 30.0):
        self.uid = uid or os.environ["PAGADITO_UID"]
        self.wsk = wsk or os.environ["PAGADITO_WSK"]
        if sandbox is None:
            raw = os.environ.get("PAGADITO_SANDBOX")
            if raw is None:
                print("⚠️ PAGADITO_SANDBOX no está definida en Railway — usando SANDBOX por seguridad")
                sandbox = True
            else:
                sandbox = raw.strip().lower() == "true"
        self.sandbox = sandbox
        self.base_url = SANDBOX_URL if sandbox else PRODUCTION_URL
        self.timeout = timeout
        self._log_ambiente()

    def _log_ambiente(self):
        """Banner en logs de Railway para saber de un vistazo a qué ambiente apunta."""
        amb = "🟡 SANDBOX (sandbox-connect.pagadito.com)" if self.sandbox \
              else "🟢 PRODUCCIÓN (connect.pagadito.com)"
        uid_mask = f"{self.uid[:8]}...{self.uid[-4:]}" if len(self.uid) >= 12 else "(uid corto?)"
        print("═" * 44)
        print(f"💳 PAGADITO → {amb}")
        print(f"   UID: {uid_mask}")
        print("═" * 44)

    # ------------------------------------------------------------------
    def _post(self, endpoint: str, payload: dict) -> dict:
        """POST con Basic Auth (UID como usuario, WSK como contraseña)."""
        url = f"{self.base_url}/{endpoint}"
        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(
                url,
                json=payload,
                auth=(self.uid, self.wsk),  # HTTP Basic Auth
                headers={"Content-Type": "application/json"},
            )
        # Pagadito puede devolver 4xx con un JSON de error útil — intentar parsear siempre
        try:
            data = r.json()
        except Exception:
            if r.status_code in (401, 403):
                raise PagaditoError("AUTH", f"Credenciales rechazadas por Pagadito (HTTP {r.status_code}). Verifica PAGADITO_UID/PAGADITO_WSK y el ambiente (sandbox vs producción).")
            r.raise_for_status()
            raise PagaditoError("HTTP", f"Respuesta no JSON de Pagadito (HTTP {r.status_code})")
        return data

    # ------------------------------------------------------------------
    def exec_trans(self, ern: str, amount: float, details: list[dict],
                   currency: str = "USD", extended_expiration: bool = False,
                   custom_params: dict | None = None) -> dict:
        """
        Registra una transacción y devuelve la URL de pago.

        details: [{"quantity": 1, "description": "Plan Mensual", "price": 29.00}, ...]
        custom_params: {"param1": "...", ...} (máx 5; requiere activarlos en el panel)

        Returns: dict con data.url y data.token
        Raises: PagaditoError si code != PG1002
        """
        payload = {
            "ern": ern,
            "amount": round(amount, 2),
            "currency": currency,
            "extended_expiration": extended_expiration,
            "details": details,
        }
        if custom_params:
            payload["custom_params"] = custom_params

        resp = self._post("exec-trans", payload)
        if resp.get("code") != CODE_TRANS_REGISTERED:
            raise PagaditoError(resp.get("code", "?"), resp.get("message", "Error desconocido"))
        return resp

    # ------------------------------------------------------------------
    def get_status(self, token: str) -> dict:
        """
        Consulta el estado de una transacción.
        Returns: dict con data.status, data.reference, data.date_trans
        Raises: PagaditoError si code != PG1003
        """
        resp = self._post("get-status", {"token": token})
        if resp.get("code") != CODE_TRANS_STATUS:
            raise PagaditoError(resp.get("code", "?"), resp.get("message", "Error desconocido"))
        return resp
