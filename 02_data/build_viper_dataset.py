"""Combina los tweets historicos con los despachos Viper de Talcahuano.

Hallazgos que fundamentan las reglas (ver PATCH_NOTES):

- El campo ``date`` de Viper esta en UTC-3 fijo (no sigue horario de invierno);
  ``dispatch.timestamp`` lo confirma. Se convierte a UTC sumando 3 horas.
- Desde 2024-06-23 todos los tweets tienen su par en Viper (mismo texto, +-15 min):
  los tweets eran un espejo del feed. Viper agrega mensajes que el bot no publico,
  sobre todo en dias de temporal.
- ``dispatch.id`` identifica la emergencia; los mensajes "SALE ..." y "SEGUNDA/
  TERCERA ALARMA" reutilizan el id. Contar mensajes sin esos prefijos reproduce
  exactamente las emergencias unicas, tambien en los tweets sin id.
- "6-6 a ..." / "Orden NN 6-6 a ..." son reclasificaciones de una emergencia
  existente y nunca se tuitearon; se excluyen.

Salidas:
- ``compiled_scraped_data_viper.csv``: Fecha;Texto;SOURCE;DISPATCH_ID;ES_MENSAJE_ADICIONAL
  (tweets antes del corte + Viper desde el corte, sin duplicados).
- ``viper_coverage.csv``: local_date;day_state para los dias del periodo Viper.
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "02_data"))

from dedup import DEFAULT_TIMEZONE, extract_incident_code, is_incident_like, normalize_text  # noqa: E402

VIPER_UTC_OFFSET = pd.Timedelta(hours=3)
CUTOFF_LOCAL_DATE = "2024-06-24"  # primer dia local completo en Viper
COVERAGE_GAP_HOURS = 30.0
EXACT_DUPLICATE_SECONDS = 300
RECLASSIFICATION_RE = re.compile(r"\b6-6\s+a\b", re.IGNORECASE)
ADDITIONAL_MESSAGE_RE = re.compile(
    r"^\s*(?:EMERGENCIA\s*:\s*)?(?:\d{1,2}:\d{2}\s*,?\s*)?"
    r"(?:SALE\b|SEGUNDA ALARMA|TERCERA ALARMA|CUARTA ALARMA)",
    re.IGNORECASE,
)


def is_additional_message(text: pd.Series) -> pd.Series:
    """Mensajes que agregan unidades a una emergencia ya contada."""
    return text.fillna("").str.match(ADDITIONAL_MESSAGE_RE)


def load_viper(folder: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(glob.glob(str(folder / "*.json"))):
        with open(path, encoding="utf-8") as stream:
            rows.extend(json.load(stream)["data"])
    frame = pd.DataFrame(rows).drop_duplicates("id")
    local = pd.to_datetime(frame["date"], format="%d-%m-%Y %H:%M:%S")
    frame["ts_utc"] = (local + VIPER_UTC_OFFSET).dt.tz_localize("UTC")
    frame["Texto"] = frame["detailTitle"].fillna("").str.replace("&#039;", "'", regex=False)
    frame["DISPATCH_ID"] = frame["dispatch"].map(
        lambda d: d.get("id") if isinstance(d, dict) else None
    )
    frame["FECHA_LOCAL"] = frame["ts_utc"].dt.tz_convert(DEFAULT_TIMEZONE).dt.strftime("%Y-%m-%d")
    return frame.sort_values("ts_utc").reset_index(drop=True)


def viper_incident_messages(frame: pd.DataFrame) -> pd.DataFrame:
    text = frame["Texto"]
    incident = [is_incident_like(t, extract_incident_code(t)) for t in text]
    keep = frame[
        pd.Series(incident, index=frame.index)
        & frame["DISPATCH_ID"].notna()
        & ~text.str.contains(RECLASSIFICATION_RE)
    ].copy()
    # Repeticiones identicas del feed a segundos de distancia.
    keep["_norm"] = keep["Texto"].map(normalize_text)
    keep = keep.sort_values("ts_utc")
    previous = keep.groupby("_norm")["ts_utc"].shift()
    repeated = (keep["ts_utc"] - previous).dt.total_seconds() <= EXACT_DUPLICATE_SECONDS
    return keep[~repeated.fillna(False)].drop(columns="_norm")


def viper_coverage(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Dias tocados por un hueco del feed (cualquier item) > COVERAGE_GAP_HOURS."""
    days = pd.date_range(start_date, end_date).strftime("%Y-%m-%d")
    state = pd.Series("observed", index=days)
    stamps = frame["ts_utc"].sort_values().reset_index(drop=True)
    gaps = stamps.diff().dt.total_seconds() / 3600
    for position in gaps[gaps > COVERAGE_GAP_HOURS].index:
        start = stamps[position - 1].tz_convert(DEFAULT_TIMEZONE).strftime("%Y-%m-%d")
        stop = stamps[position].tz_convert(DEFAULT_TIMEZONE).strftime("%Y-%m-%d")
        touched = pd.date_range(start, stop).strftime("%Y-%m-%d")
        state[state.index.isin(touched)] = "coverage_unknown"
    return state.rename_axis("local_date").rename("day_state").reset_index()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--viper-dir", type=Path, default=ROOT / "01_scraper" / "talcahuano")
    parser.add_argument("--tweets", type=Path, default=ROOT / "02_data" / "compiled_scraped_data_twitter.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "02_data" / "compiled_scraped_data_viper.csv")
    parser.add_argument("--coverage-output", type=Path, default=ROOT / "02_data" / "viper_coverage.csv")
    parser.add_argument("--viper-only", action="store_true",
                        help="Sin tweets: la serie parte el primer dia local completo del feed (p. ej. Concepcion)")
    args = parser.parse_args()

    viper = load_viper(args.viper_dir)
    cutoff = CUTOFF_LOCAL_DATE
    if args.viper_only:
        cutoff = (pd.Timestamp(viper["FECHA_LOCAL"].min()) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = viper["FECHA_LOCAL"].max()
    if viper["ts_utc"].max().tz_convert(DEFAULT_TIMEZONE).hour < 23:
        end_date = (pd.Timestamp(end_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    incidents = viper_incident_messages(viper)
    incidents = incidents[
        (incidents["FECHA_LOCAL"] >= cutoff) & (incidents["FECHA_LOCAL"] <= end_date)
    ]

    tweets = pd.read_csv(args.tweets, sep=";")
    tweet_local = pd.to_datetime(tweets["Fecha"], utc=True).dt.tz_convert(DEFAULT_TIMEZONE)
    tweets = tweets[tweet_local.dt.strftime("%Y-%m-%d") < cutoff].copy()
    if args.viper_only:
        tweets = tweets.iloc[0:0]
    tweets["SOURCE"] = "twitter"
    tweets["DISPATCH_ID"] = pd.NA

    viper_rows = pd.DataFrame({
        "Fecha": incidents["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "Texto": incidents["Texto"],
        "SOURCE": "viper",
        "DISPATCH_ID": incidents["DISPATCH_ID"].astype("Int64"),
    })
    combined = pd.concat([tweets.astype({"DISPATCH_ID": "Int64"}), viper_rows], ignore_index=True)
    combined["ES_MENSAJE_ADICIONAL"] = is_additional_message(combined["Texto"]).astype(int)
    combined.to_csv(args.output, sep=";", index=False)

    coverage = viper_coverage(viper, cutoff, end_date)
    coverage.to_csv(args.coverage_output, index=False)

    viper_ids = incidents.loc[~is_additional_message(incidents["Texto"]), "DISPATCH_ID"]
    print(f"Viper: {len(viper)} items -> {len(incidents)} mensajes de emergencia "
          f"({cutoff}..{end_date}); {incidents['DISPATCH_ID'].nunique()} dispatch ids, "
          f"{viper_ids.nunique()} mensajes iniciales")
    print(f"Tweets conservados (antes de {cutoff}): {len(tweets)}")
    print(f"Combinado: {len(combined)} filas -> {args.output}")
    print(f"Cobertura Viper: {coverage['day_state'].value_counts().to_dict()} -> {args.coverage_output}")


if __name__ == "__main__":
    main()
