"""
Cliente Supabase compartido para todo el backend.

POR QUÉ EXISTE ESTE ARCHIVO
---------------------------
supabase-py (via postgrest-py) crea por defecto un httpx.Client con http2=True.
PostgREST cierra las conexiones inactivas a los ~30 s ("Warp server error:
Thread killed by timeout manager" en los logs de Supabase). Con HTTP/2, httpx
no detecta ese cierre hasta que intenta reusar la conexión muerta y entonces
lanza `ConnectionTerminated error_code:0` → el scraper falla cada ciclo y los
endpoints devuelven 500 de forma intermitente.

Solución:
  1. http2=False  → HTTP/1.1; httpx detecta la conexión cerrada y abre otra.
  2. keepalive_expiry=15 s → httpx descarta las conexiones ociosas ANTES de
     que PostgREST las mate (~30 s), así nunca reusa una muerta.

USO
---
    from supabase_client import crear_cliente
    supabase       = crear_cliente(SUPABASE_URL, SUPABASE_KEY)
    supabase_admin = crear_cliente(SUPABASE_URL, SUPABASE_SERVICE_KEY)

Misma firma que `supabase.create_client`, así el cambio en cada módulo es
solo reemplazar el import y el nombre de la función.
"""
import httpx
from supabase import Client, create_client

try:
    from supabase import ClientOptions          # supabase >= 2.x
except ImportError:                             # versiones antiguas
    from supabase.lib.client_options import ClientOptions


def _http_client() -> httpx.Client:
    return httpx.Client(
        http2=False,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(
            max_connections=20,
            max_keepalive_connections=5,
            keepalive_expiry=15.0,   # < 30 s del timeout de PostgREST
        ),
        follow_redirects=True,
    )


def crear_cliente(url: str, key: str) -> Client:
    """Reemplazo directo de supabase.create_client con transporte HTTP/1.1."""
    return create_client(url, key, options=ClientOptions(httpx_client=_http_client()))
