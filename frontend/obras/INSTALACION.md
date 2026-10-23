# Control de Obra — instalación y plan de la semana

Módulo interno (CONSER) equivalente al núcleo de SINCO ADPRO: **contratado vs. ejecutado vs. gastado**, por partida, capítulo y obra. Mismo stack que LicitacionLab: FastAPI + Supabase + frontend vanilla, mismo patrón que el Bid Manager.

## Archivos nuevos

| Archivo | Qué es |
|---|---|
| `migrations/2026-10-23_obras_fase0.sql` | 5 tablas (`obras`, `obra_partidas`, `obra_cubicaciones`, `obra_cubicacion_detalle`, `obra_gastos`) y 2 vistas (`v_obra_partida_estado`, `v_obra_resumen`) |
| `router_obras.py` | API `/api/obras` (CRUD + tablero) |
| `frontend/obras/obras.html` | Markup del módulo (lista de obras + detalle con 4 pestañas) |
| `frontend/obras/obras.js` | Objeto `Obras` (lógica) |
| `frontend/obras/obras.css` | Estilos `ob-*`; reutiliza `bm-*` de bid-manager.css |

## Instalación (10 minutos)

**1. Migración.** Ejecuta `migrations/2026-10-23_obras_fase0.sql` en Supabase (MCP o SQL Editor). Es idempotente.

**2. Backend.** En `main.py`, junto a los otros routers:

```python
from router_obras import obras_router
app.include_router(obras_router)
```

**3. Frontend.** En `frontend/index.html`:

- Junto al `<link>` de bid-manager.css (línea ~3028):
  ```html
  <link rel="stylesheet" href="/frontend/obras/obras.css">
  ```
- Junto a `<div id="tab-bid"></div>` (línea ~4298):
  ```html
  <div id="tab-obras"></div>
  ```
- En el sidebar, debajo del botón `dsb-nav-bid` (línea ~3405):
  ```html
  <button class="dsb-nav-item" id="dsb-nav-obras" onclick="switchTab2('obras')">Control de Obra</button>
  ```
- En `switchTab2`: agrega `'obras'` al array `allTabs` (línea ~7637), la línea
  ```js
  if (tab==='obras') { if (typeof Obras !== 'undefined') Obras.init(); }
  ```
  junto a la de `bid` (línea ~7668), y `'obras':'dsb-nav-obras'` en `dsbMap` (línea ~7682).
- Junto a los `<script>` del bid-manager (línea ~13130):
  ```html
  <script src="/frontend/obras/obras.js" defer></script>
  ```

**4. Prueba.** Crea una obra, pega el listado de cantidades desde Excel (código · descripción · unidad · cantidad · PU, separado por tabulador; las filas solo con texto se toman como capítulo), crea la Cubicación No. 1, márcala presentada, carga dos o tres gastos a partidas y mira el tablero.

## Modelo de datos (lo que SINCO llama ADPRO, reducido a lo esencial)

```
obras ──< obra_partidas ──< obra_cubicacion_detalle >── obra_cubicaciones
   │            │
   └──< obra_gastos (partida_id opcional; NULL = indirecto)
```

- **Ejecutado** = Σ cantidad cubicada × PU, solo de cubicaciones en estado presentada/aprobada/pagada.
- **Gastado** = Σ gastos cargados a la partida. Los que no tienen partida son indirectos de obra.
- **Margen** = ejecutado − gastado. Negativo por partida = alerta en el tablero.
- **CU real / APU** = gastado ÷ cantidad ejecutada, comparado con el costo del APU si lo cargaste. Es el dato que ningún Excel de obra te da sin pelear.

Todo el tablero sale de dos vistas SQL; el router solo agrupa por capítulo y arma las alertas.

## Plan de la semana (tú solo)

| Día | Entregable |
|---|---|
| 1 | Migración + router + frontend instalados y probados con una obra real de CONSER (la más reciente). Ajustar el parser de pegado a tu formato de listado. |
| 2 | Importar partidas desde el Excel del presupuesto directamente (openpyxl: hoja LISTADO → partidas; hoja APU → `costo_unitario_apu`). Así el presupuesto de licitación entra sin retipear. |
| 3 | Exportar la cubicación a Excel/PDF con el formato que pide la entidad (tabla cantidad anterior / actual / acumulada). Es lo que presentas al supervisor. |
| 4 | Gastos desde el celular: formulario corto + foto de la factura a Supabase Storage (reusar `bid_archivos`). Que el encargado de obra lo cargue él. |
| 5 | Curva S: cronograma del MS Project (exportar a CSV) contra ejecutado real por semana. Gráfica sencilla en el tablero. |
| 6 | Cierre: retención, anticipo y amortización en `obra_cubicaciones` (campos + cálculo del neto a cobrar), y vista de flujo de caja por obra. |
| 7 | Revisión con uso real, corrección de lo que moleste, y decidir el siguiente módulo (proveedores/compras o maquinaria). |

## Lo que NO entra ahora (y por qué)

- Contabilidad y factura electrónica (e-CF): se integran con el contable, no se construyen.
- Nómina: es un módulo entero; de momento la mano de obra entra como gasto tipo `mano_obra`.
- Multiempresa: ya existe `empresa_id` en todo, así que cuando quieras venderlo solo falta UI de permisos.
