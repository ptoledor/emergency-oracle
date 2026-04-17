"""
Scraper de tweets de @Central_CBT usando Playwright + stealth.
Guarda progreso incrementalmente para poder resumir si se corta.
Genera un CSV por dia en scraper/scraped_data/.

Uso:
    python twitter_scraper.py             # scrapea todos los dias pendientes
    python twitter_scraper.py --reset     # borra progreso y empieza de cero

Cuando detecta rate limit real (via probe):
    - Revierte los ultimos dias marcados como done
    - Guarda progreso y cierra
    - Actualiza cookies.json y vuelve a ejecutar para retomar
"""

import argparse
import asyncio
import json
import logging
import random
import pandas as pd
from collections import deque
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

TARGET     = "Central_CBT"
START_DATE = date(2024, 1, 1)
END_DATE   = date.today() + timedelta(days=1)

SCRAPED_DIR   = Path(__file__).parent / "scraped_data"
PROGRESS_FILE = SCRAPED_DIR / "progress.json"
COOKIES_FILE  = Path(__file__).parent / "cookies.json"
LOG_FILE      = SCRAPED_DIR / "scraper.log"


MAX_SCROLLS = 20  # maximo de scrolls por consulta diaria
PROBE_SINCE = "2024-01-09"  # dia conocido con tweets, para verificar rate limit
PROBE_UNTIL = "2024-01-10"


def setup_logging():
    SCRAPED_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cookies helpers
# ---------------------------------------------------------------------------

def parse_cookie_editor(raw: list[dict]) -> list[dict]:
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


def load_cookies() -> list[dict]:
    with open(COOKIES_FILE) as f:
        return parse_cookie_editor(json.load(f))



# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def day_ranges(start: date, end: date):
    current = start
    while current < end:
        next_day = current + timedelta(days=1)
        yield current.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d")
        current = next_day


def load_progress() -> set:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("done", []))
    return set()


def save_daily_file(rows: list, since: str):
    SCRAPED_DIR.mkdir(parents=True, exist_ok=True)
    path = SCRAPED_DIR / f"tweets_{since}.csv"
    if rows:
        df = pd.DataFrame(rows)[["Fecha", "Texto"]].drop_duplicates(subset=["Fecha", "Texto"])
        df = df.sort_values("Fecha").reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["Fecha", "Texto"])
    df.to_csv(path, index=False, sep=";", decimal=",")


def save_progress(done_periods: set):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"done": sorted(done_periods)}, f)


# ---------------------------------------------------------------------------
# Playwright helpers
# ---------------------------------------------------------------------------

def extract_from_body(body: bytes) -> tuple[list[dict], bool]:
    rows = []
    try:
        data = json.loads(body)
        for err in data.get("errors", []):
            code = err.get("code", 0)
            msg = str(err.get("message", "")).lower()
            if code == 88 or code == 429 or "rate limit" in msg or "rate_limit" in msg:
                return [], True
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
    return rows, False


async def make_context(browser, cookies: list[dict]):
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="es-CL",
        timezone_id="America/Santiago",
    )
    await ctx.add_cookies(cookies)
    return ctx


async def scrape_period(context, since: str, until: str) -> tuple[list[dict], bool, bool]:
    """Retorna (tweets, got_response, rate_limited)."""
    page = await context.new_page()
    await Stealth().apply_stealth_async(page)

    pending_responses: list = []

    def handle_response(response):
        if "SearchTimeline" in response.url:
            pending_responses.append(response)

    page.on("response", handle_response)
    encoded = f"from%3A{TARGET}%20since%3A{since}%20until%3A{until}"
    url = f"https://x.com/search?q={encoded}&src=typed_query&f=live"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)
        prev_count = 0
        no_new = 0
        for _ in range(MAX_SCROLLS):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            if len(pending_responses) == prev_count:
                no_new += 1
                if no_new >= 2:
                    break
            else:
                no_new = 0
                prev_count = len(pending_responses)
    except Exception as e:
        log.error(f"[!] Navegacion fallida: {e}")

    collected = []
    is_rate_limited = False
    for resp in pending_responses:
        try:
            if resp.status == 429:
                is_rate_limited = True
                continue
            body = await resp.body()
            if body:
                rows, rl = extract_from_body(body)
                collected.extend(rows)
                if rl:
                    is_rate_limited = True
        except Exception:
            pass

    await page.close()
    return collected, len(pending_responses) > 0, is_rate_limited


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

async def scrape(args):
    done_periods = load_progress()
    all_ranges = list(day_ranges(START_DATE, END_DATE))
    total = len(all_ranges)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        active_cookies = load_cookies()
        context = await make_context(browser, active_cookies)

        while True:
            pending = [(s, u) for s, u in all_ranges if s not in done_periods]
            if not pending:
                break
            log.info(f"[+] Total: {total} dias | Pendientes: {len(pending)}")

            recently_done: deque[str] = deque(maxlen=2)
            need_new_cookies = False
            need_retry = False

            for i, (since, until) in enumerate(pending):
                seq = all_ranges.index((since, until)) + 1
                rows, got_response, rate_limited = await scrape_period(context, since, until)

                if not got_response:
                    log.warning(f"  [{seq:03d}/{total}] {since} SIN_RESPUESTA — reiniciando, reintentando...")
                    await context.close()
                    context = await make_context(browser, active_cookies)
                    await asyncio.sleep(15)
                    rows, got_response, rate_limited = await scrape_period(context, since, until)

                if got_response:
                    done_periods.add(since)
                    recently_done.append(since)
                    save_daily_file(rows, since)
                    save_progress(done_periods)
                    status = " RATE_LIMIT?" if rate_limited else ""
                    log.info(f"  [{seq:03d}/{total}] {since}  {len(rows)} tweets{status}")
                else:
                    done_periods.add(since)
                    recently_done.append(since)
                    save_progress(done_periods)
                    log.warning(f"  [{seq:03d}/{total}] {since}  SIN_RESPUESTA x2 — saltando")

                # verificar rate limit real con dia probe conocido
                confirmed_rate_limit = False
                if rate_limited and len(rows) == 0:
                    probe_rows, _, _ = await scrape_period(context, PROBE_SINCE, PROBE_UNTIL)
                    confirmed_rate_limit = len(probe_rows) == 0
                    log.info(f"  [probe] {len(probe_rows)} tweets — {'RATE LIMIT CONFIRMADO' if confirmed_rate_limit else 'mes vacio normal'}")

                if confirmed_rate_limit:
                    rollback = list(recently_done)
                    for d in rollback:
                        done_periods.discard(d)
                        csv = SCRAPED_DIR / f"tweets_{d}.csv"
                        if csv.exists():
                            csv.unlink()
                    recently_done.clear()
                    save_progress(done_periods)
                    log.warning(f"[!] Revertidos y CSVs eliminados: {rollback}")
                    log.warning(f"[!] Rate limit confirmado — esperando 1 minuto y reintentando...")
                    await context.close()
                    await asyncio.sleep(60)
                    context = await make_context(browser, active_cookies)
                    need_retry = True
                    break

                wait = random.uniform(5, 45)
                await asyncio.sleep(wait)

            if need_retry:
                need_retry = False
                continue
            break  # finalizado sin rate limit

        await context.close()
        await browser.close()

    log.info(f"[+] Finalizado. Datos en: {SCRAPED_DIR}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Borrar progreso y empezar de cero")
    args = parser.parse_args()

    if args.reset:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
        for f in SCRAPED_DIR.glob("tweets_*.csv"):
            f.unlink()
        print("[+] Progreso borrado.")

    setup_logging()
    asyncio.run(scrape(args))


if __name__ == "__main__":
    main()
