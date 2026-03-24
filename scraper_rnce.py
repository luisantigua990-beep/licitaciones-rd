"""
scraper_rnce.py
Agente Prospector — Scraping del RNCE (Registro Nacional de Contratistas y Ejecutores)
Llamado por n8n vía HTTP request al endpoint /api/agentes/prospector/run

Fuentes:
  1. RNCE — empresas registradas como contratistas del Estado
  2. DGCP adjudicaciones — historial de contratos ganados (ya tienes acceso vía tu API)
"""

import asyncio
import re
from playwright.async_api import async_playwright

RNCE_URL = "https://rnce.dgcp.gob.do/listado-empresas"


async def scrape_rnce(max_empresas: int = 100) -> list[dict]:
    """
    Extrae empresas del RNCE.
    Retorna lista de dicts con: nombre, rnc, categoria, region, telefono, email, web
    """
    empresas = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()

        try:
            await page.goto(RNCE_URL, wait_until="networkidle", timeout=30000)
            await page.wait_for_selector("table, .empresa-card, .listado", timeout=15000)

            pagina = 1
            while len(empresas) < max_empresas:
                # Extraer filas de la tabla (ajustar selector según DOM real)
                filas = await page.query_selector_all("tbody tr, .empresa-item")

                for fila in filas:
                    try:
                        # Intentar extraer celdas — ajustar selectores al DOM real del RNCE
                        celdas = await fila.query_selector_all("td")
                        if len(celdas) < 3:
                            continue

                        nombre = (await celdas[0].inner_text()).strip()
                        rnc    = (await celdas[1].inner_text()).strip()
                        categ  = (await celdas[2].inner_text()).strip() if len(celdas) > 2 else ""
                        region = (await celdas[3].inner_text()).strip() if len(celdas) > 3 else ""

                        if not nombre or not rnc:
                            continue

                        empresas.append({
                            "nombre":    nombre,
                            "rnc":       re.sub(r"[^0-9]", "", rnc),
                            "categoria": categ,
                            "region":    region,
                            "telefono":  "",
                            "email":     "",
                            "web":       "",
                            "fuente":    "rnce",
                        })

                    except Exception:
                        continue

                if len(empresas) >= max_empresas:
                    break

                # Intentar pasar a la siguiente página
                siguiente = await page.query_selector("a[aria-label='Next'], .pagination .next, button:has-text('Siguiente')")
                if not siguiente:
                    break

                await siguiente.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                pagina += 1

        except Exception as e:
            print(f"[RNCE scraper] Error en página {pagina}: {e}")

        finally:
            await browser.close()

    return empresas[:max_empresas]


async def buscar_google_maps(tipo: str = "empresa constructora", provincia: str = "Santo Domingo", max_resultados: int = 50) -> list[dict]:
    """
    Busca empresas en Google Maps por tipo y provincia.
    Alternativa al RNCE para encontrar empresas que no están registradas todavía.
    Nota: usar Google Maps API es más confiable — esto es el fallback con Playwright.
    """
    empresas = []
    query = f"{tipo} {provincia} República Dominicana"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        page = await browser.new_page()

        try:
            await page.goto(
                f"https://www.google.com/maps/search/{query.replace(' ', '+')}",
                wait_until="networkidle",
                timeout=30000
            )
            await page.wait_for_selector('[role="feed"]', timeout=15000)

            # Scroll para cargar más resultados
            for _ in range(5):
                await page.evaluate('document.querySelector(\'[role="feed"]\').scrollBy(0, 1000)')
                await asyncio.sleep(1.5)

            items = await page.query_selector_all('[role="feed"] > div')

            for item in items[:max_resultados]:
                try:
                    nombre_el = await item.query_selector("a[aria-label]")
                    if not nombre_el:
                        continue

                    nombre = await nombre_el.get_attribute("aria-label")
                    if not nombre:
                        continue

                    empresas.append({
                        "nombre":    nombre.strip(),
                        "rnc":       "",
                        "categoria": tipo,
                        "region":    provincia,
                        "telefono":  "",
                        "email":     "",
                        "web":       "",
                        "fuente":    "google_maps",
                    })

                except Exception:
                    continue

        except Exception as e:
            print(f"[Google Maps scraper] Error: {e}")

        finally:
            await browser.close()

    return empresas


async def run_prospector(max_rnce: int = 80, max_maps: int = 40) -> list[dict]:
    """
    Punto de entrada principal.
    Combina RNCE + Google Maps y deduplica por nombre.
    """
    print("[Prospector] Iniciando scraping RNCE...")
    rnce = await scrape_rnce(max_empresas=max_rnce)
    print(f"[Prospector] RNCE: {len(rnce)} empresas encontradas")

    print("[Prospector] Iniciando búsqueda Google Maps...")
    provincias = ["Santo Domingo", "Santiago", "La Vega", "San Pedro de Macorís"]
    maps = []
    for prov in provincias:
        resultado = await buscar_google_maps(provincia=prov, max_resultados=max_maps // len(provincias))
        maps.extend(resultado)
    print(f"[Prospector] Google Maps: {len(maps)} empresas encontradas")

    # Deduplicar por nombre normalizado
    todas = rnce + maps
    vistos = set()
    unicas = []
    for e in todas:
        clave = re.sub(r"[^a-z0-9]", "", e["nombre"].lower())
        if clave not in vistos:
            vistos.add(clave)
            unicas.append(e)

    print(f"[Prospector] Total únicas: {len(unicas)}")
    return unicas


if __name__ == "__main__":
    # Test local
    resultado = asyncio.run(run_prospector(max_rnce=10, max_maps=10))
    for e in resultado[:5]:
        print(e)
