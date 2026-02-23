# Licitaciones RD — Sistema de Notificaciones

Sistema que monitorea la API de la DGCP (Dirección General de Contrataciones Públicas) de República Dominicana y notifica a los usuarios sobre nuevos procesos de compras públicas según sus intereses.

## Stack
- **Backend:** Python + FastAPI
- **Base de datos:** Supabase
- **Datos:** API DGCP (datosabiertos.dgcp.gob.do)
- **Deploy:** Railway

## Endpoints
- `GET /` — Health check
- `GET /api/procesos` — Listar procesos con filtros
- `GET /api/procesos/{codigo}` — Detalle de proceso con artículos
- `GET /api/procesos/por-rubros` — Buscar por códigos UNSPSC
- `GET /api/catalogo/segmentos` — Segmentos UNSPSC
- `GET /api/catalogo/familias/{segmento}` — Familias de un segmento
- `GET /api/catalogo/clases/{familia}` — Clases de una familia
- `GET /api/stats` — Estadísticas generales

## Variables de entorno
```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=eyJ...
```
