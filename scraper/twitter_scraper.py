"""
Scraper de tweets de @Central_CBT usando Playwright + stealth.
Guarda progreso incrementalmente para poder resumir si se corta.
Genera un CSV por mes en data/raw/mensual/.

Uso:
    python twitter_scraper.py             # scrapea todos los meses pendientes
    python twitter_scraper.py --reset     # borra progreso y empieza de cero

Cuando detecta 2 RATE_LIMIT seguidos:
    - Revierte los ultimos 2 dias marcados como done
    - Espera que el usuario deje cookies nuevas en data/raw/new_cookies.json
    - Retoma automaticamente al detectar el archivo
"""

import argparse
import asyncio
import json
import pandas as pd
from collections import deque
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

TARGET = "Central_CBT"
START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 1, 1)
STEP_DAYS = 1

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_RAW     = DATA_DIR / "raw" / "tweets_raw.csv"
OUTPUT_PROCESSED = DATA_DIR / "processed" / "tweets_procesados.csv"
OUTPUT_MENSUAL = DATA_DIR / "raw" / "mensual"
PROGRESS_FILE  = DATA_DIR / "raw" / "progress.json"
COOKIE_FILE    = DATA_DIR / "raw" / "new_cookies.json"

PATRON_CODIGO  = r"\b(\d+-\d+-\d+)\b"
PATRON_ALFANUM = r"\b[A-Z]+-?\d+\b"

COOKIES = [
    {"name": "auth_token",         "value": "a2a7a7bff5c8969f9a8548be5cc033b03e397994",                                                                                                                                                                                                                              "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": True,  "sameSite": "None"},
    {"name": "ct0",                "value": "39f567f53c9d99e7b7b76e35793fc95afc3c586036a4b0fda41ffd9c5de7ac93163259ba830bc70a2a6da5a27189f0c4c8792cd9b7a55fd774632c0289d3b1eb03024f80cfbe2f8558a08f448d233da1",                                                                                          "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "Lax"},
    {"name": "twid",               "value": "u%3D1937743479133626368",                                                                                                                                                                                                                                               "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "kdt",                "value": "8Q7f64mL3Y5KeGcu5r4hZKJKxSngIg6hwrdA4eYk",                                                                                                                                                                                                                              "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": True,  "sameSite": "None"},
    {"name": "att",                "value": "1-Lxpj31po4N7uY2g4x8MV8oEOLRlBYDz0zhWz5chg",                                                                                                                                                                                                                           "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": True,  "sameSite": "None"},
    {"name": "gt",                 "value": "2044983209578967112",                                                                                                                                                                                                                                                   "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "guest_id",           "value": "v1%3A177639696507283236",                                                                                                                                                                                                                                               "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "guest_id_ads",       "value": "v1%3A177639696507283236",                                                                                                                                                                                                                                               "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "guest_id_marketing", "value": "v1%3A177639696507283236",                                                                                                                                                                                                                                               "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "personalization_id", "value": "\"v1_E1P0mdp8DRyI51pmrTmvRw==\"",                                                                                                                                                                                                                                      "domain": ".x.com", "path": "/", "secure": True,  "httpOnly": False, "sameSite": "None"},
    {"name": "__cuid",             "value": "0f7591d906b945a8a2bec4e64b245c15",                                                                                                                                                                                                                                     "domain": ".x.com", "path": "/", "secure": False, "httpOnly": False, "sameSite": "Lax"},
]

BATCH_SIZE  = 5    # periodos por batch antes de reiniciar el contexto
BATCH_PAUSE = 120  # segundos de pausa entre batches


# ---------------------------------------------------------------------------
# Cookies helpers
# ---------------------------------------------------------------------------

def parse_cookie_editor(raw: list[dict]) -> list[dict]:
    """Convierte formato Cookie-Editor a formato Playwright."""
    mapping = {"no_restriction": "None", "lax": "Lax", "strict": "Strict"}
    result = []
    for c in raw:
        if not c.get("name") or c.get("value") is None:
            continue
        domain = c.get("domain", ".x.com")
        same_site = mapping.get((c.get("sameSite") or "").lower(), "None")
        result.append({
            "name": c["name"],
            "value": str(c["value"]),
            "domain": domain,
            "path": c.get("path", "/"),
            "secure": bool(c.get("secure", False)),
            "httpOnly": bool(c.get("httpOnly", False)),
            "sameSite": same_site,
        })
    return result


async def wait_for_cookies() -> list[dict] | None:
    """Pausa hasta que el usuario deje new_cookies.json. Retorna cookies parseadas."""
    if COOKIE_FILE.exists():
        COOKIE_FILE.unlink()

    print(f"\n{'='*62}")
    print(f"  ACCION REQUERIDA — rate limit detectado")
    print(f"  1. Exporta las cookies de x.com con Cookie-Editor")
    print(f"  2. Guarda el JSON en:")
    print(f"     {COOKIE_FILE}")
    print(f"  El scraper retoma automaticamente al detectar el archivo.")
    print(f"{'='*62}\n")

    while not COOKIE_FILE.exists():
        await asyncio.sleep(5)

    await asyncio.sleep(1)
    try:
        with open(COOKIE_FILE) as f:
            raw = json.load(f)
        cookies = parse_cookie_editor(raw)
        COOKIE_FILE.unlink()
        print(f"[+] Cookies nuevas cargadas ({len(cookies)} cookies). Retomando...\n")
        return cookies
    except Exception as e:
        print(f"[!] Error leyendo {COOKIE_FILE.name}: {e}. Continuando con cookies anteriores.")
        return None


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def procesar(df: pd.DataFrame) -> pd.DataFrame:
    df["FECHA_DIA"]  = df["Fecha"].astype(str).str[:10]
    df["FECHA_HORA"] = df["Fecha"].astype(str).str[11:19]
    df["CODIGO"]     = df["Texto"].str.extract(PATRON_CODIGO)
    df["codigo_alfanumerico"] = df["Texto"].str.findall(PATRON_ALFANUM).str.join(", ")
    return df


def day_ranges(start: date, end: date, step_days: int = 1):
    current = start
    while current < end:
        next_step = min(current + timedelta(days=step_days), end)
        yield current.strftime("%Y-%m-%d"), next_step.strftime("%Y-%m-%d")
        current = next_step


def load_progress() -> tuple[set, list]:
    done = set()
    all_rows = []
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done = set(json.load(f).get("done", []))
    if OUTPUT_RAW.exists():
        df = pd.read_csv(OUTPUT_RAW, sep=";")
        all_rows = df.to_dict("records")
    return done, all_rows


def save_monthly_file(all_rows: list, year_month: str):
    OUTPUT_MENSUAL.mkdir(parents=True, exist_ok=True)
    rows_mes = [r for r in all_rows if str(r.get("Fecha", "")).startswith(year_month)]
    if not rows_mes:
        return
    df = pd.DataFrame(rows_mes)[["Fecha", "Texto"]].drop_duplicates(subset=["Fecha", "Texto"])
    df = df.sort_values("Fecha", ascending=False).reset_index(drop=True)
    df.to_csv(OUTPUT_MENSUAL / f"tweets_{year_month}.csv", index=False, sep=";", decimal=",")


def save_progress(done_periods: set, all_rows: list, last_since: str | None = None) -> int:
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"done": sorted(done_periods)}, f)
    if not all_rows:
        return 0
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["Fecha", "Texto"])
    df = df.sort_values("Fecha", ascending=False).reset_index(drop=True)
    df.to_csv(OUTPUT_RAW, index=False, sep=";", decimal=",")
    procesar(df.copy()).to_csv(OUTPUT_PROCESSED, index=False, sep=";", decimal=",")
    if last_since:
        save_monthly_file(df.to_dict("records"), last_since[:7])
    return len(df)


# ---------------------------------------------------------------------------
# Playwright helpers
# ---------------------------------------------------------------------------

def extract_from_body(body: bytes) -> list[dict]:
    rows = []
    try:
        data = json.loads(body)
        instructions = (
            data["data"]["search_by_raw_query"]["search_timeline"]["timeline"]["instructions"]
        )
        for inst in instructions:
            if inst.get("type") != "TimelineAddEntries":
                continue
            for entry in inst.get("entries", []):
                try:
                    legacy = entry["content"]["itemContent"]["tweet_results"]["result"]["legacy"]
                    dt = datetime.strptime(legacy["created_at"], "%a %b %d %H:%M:%S %z %Y")
                    iso = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    text = legacy.get("full_text") or legacy.get("text", "")
                    rows.append({"Fecha": iso, "Texto": text})
                except (KeyError, ValueError):
                    continue
    except (KeyError, json.JSONDecodeError):
        pass
    return rows


async def make_context(browser, cookies: list[dict] | None = None):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="es-CL",
        timezone_id="America/Santiago",
    )
    await ctx.add_cookies(cookies or COOKIES)
    return ctx


async def scrape_period(context, since: str, until: str) -> tuple[list[dict], bool, bool]:
    """Retorna (tweets, got_response, rate_limited)."""
    page = await context.new_page()
    await Stealth().apply_stealth_async(page)

    # handler sincrono: solo guarda referencias, procesa cuerpos despues del loop
    pending_responses: list = []

    def handle_response(response):
        if "SearchTimeline" in response.url:
            pending_responses.append(response)

    page.on("response", handle_response)
    encoded = f"from%3A{TARGET}%20since%3A{since}%20until%3A{until}"
    url = f"https://x.com/search?q={encoded}&src=typed_query&f=live"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)
        for _ in range(12):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            if len(pending_responses) == 0 and _ >= 2:
                break
    except Exception as e:
        print(f"[!] Navegacion fallida: {e}")

    # procesar cuerpos antes de cerrar la pagina
    collected = []
    for resp in pending_responses:
        try:
            body = await resp.body()
            if body:
                collected.extend(extract_from_body(body))
        except Exception:
            pass

    await page.close()

    got_response = len(pending_responses) > 0
    rate_limited = got_response and len(collected) == 0
    return collected, got_response, rate_limited


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

async def scrape(args):
    global COOKIES
    done_periods, all_rows = load_progress()
    all_ranges = list(day_ranges(START_DATE, END_DATE, STEP_DAYS))
    total = len(all_ranges)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        active_cookies = list(COOKIES)
        context = await make_context(browser, active_cookies)

        while True:
            pending = [(s, u) for s, u in reversed(all_ranges) if s not in done_periods]
            if not pending:
                break
            print(f"[+] Periodos de {STEP_DAYS} dia(s) | Total: {total} | Pendientes: {len(pending)}")

            recently_done: deque[str] = deque(maxlen=2)
            consecutive_empty = 0
            since_last_pause = 0
            need_new_cookies = False

            for i, (since, until) in enumerate(pending):
                seq = total - all_ranges.index((since, until))
                print(f"  [{seq:03d}/{total}] {since} -> {until}", end=" ... ", flush=True)

                rows, got_response, rate_limited = await scrape_period(context, since, until)

                # contexto bloqueado — reintentar una vez
                if not got_response:
                    print("SIN_RESPUESTA — reiniciando, reintentando...", end=" ", flush=True)
                    await context.close()
                    context = await make_context(browser, active_cookies)
                    consecutive_empty = 0
                    since_last_pause = 0
                    await asyncio.sleep(15)
                    rows, got_response, rate_limited = await scrape_period(context, since, until)

                all_rows.extend(rows)

                if got_response:
                    done_periods.add(since)
                    recently_done.append(since)
                    n_unique = save_progress(done_periods, all_rows, last_since=since) or 0
                    status = "RATE_LIMIT?" if rate_limited else ""
                    print(f"{len(rows)} nuevos | {n_unique} total | {since[:7]} {status}")
                else:
                    # segundo fallo: saltar para no quedar trabado
                    done_periods.add(since)
                    recently_done.append(since)
                    save_progress(done_periods, all_rows)
                    print(f"SIN_RESPUESTA x2 — saltando")

                consecutive_empty = consecutive_empty + 1 if len(rows) == 0 else 0

                # 2 vacios seguidos con rate limit → pedir cookies nuevas
                if consecutive_empty >= 2 and rate_limited:
                    rollback = list(recently_done)
                    for d in rollback:
                        done_periods.discard(d)
                    recently_done.clear()
                    save_progress(done_periods, all_rows)
                    print(f"\n    [!] Revertidos: {rollback}")

                    await context.close()
                    new_cookies = await wait_for_cookies()
                    if new_cookies:
                        active_cookies = new_cookies
                        COOKIES = new_cookies
                    context = await make_context(browser, active_cookies)
                    consecutive_empty = 0
                    since_last_pause = 0
                    need_new_cookies = True
                    break  # reconstruir pending con rollback aplicado

                since_last_pause += 1
                if since_last_pause >= BATCH_SIZE and i < len(pending) - 1:
                    remaining = len(pending) - i - 1
                    print(f"\n    [pausa {BATCH_PAUSE}s -- {remaining} dias pendientes]\n")
                    await context.close()
                    await asyncio.sleep(BATCH_PAUSE)
                    context = await make_context(browser, active_cookies)
                    since_last_pause = 0

            if not need_new_cookies:
                break  # finalizado

        await context.close()
        await browser.close()

    print(f"\n[+] Finalizado.")
    print(f"[+] Raw:      {OUTPUT_RAW}")
    print(f"[+] Procesado:{OUTPUT_PROCESSED}")
    print(f"[+] Mensuales:{OUTPUT_MENSUAL}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Borrar progreso y empezar de cero")
    args = parser.parse_args()

    if args.reset:
        for f in [OUTPUT_RAW, OUTPUT_PROCESSED, PROGRESS_FILE]:
            if f.exists():
                f.unlink()
        if OUTPUT_MENSUAL.exists():
            for f in OUTPUT_MENSUAL.glob("tweets_*.csv"):
                f.unlink()
        print("[+] Progreso borrado.")

    asyncio.run(scrape(args))


if __name__ == "__main__":
    main()
