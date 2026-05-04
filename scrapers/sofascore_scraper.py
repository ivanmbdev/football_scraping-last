#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrapers/sofascore_scraper.py
=============================
Scraper unificado de SofaScore. Sigue el mismo patrón que *understat_scraper.py*:

    1. CONSTANTS            – configuración del scraper
    2. HELPERS              – driver Selenium y petición JSON
    3. FETCH FUNCTIONS      – funciones puras de obtención de datos
    4. ORCHESTRATOR         – scrape_sofascore() acumula todo
    5. TRANSFORM            – adapta campos al esquema de la DB
    6. DIM EXTRACTORS      – extract_teams(), extract_players()
    7. MAIN                 – scrape → transform → guardar en disco
    8. __main__ guard

Salida (data/raw/sofascore/):
    season=<label>/
        matches_batch_<id>.json          <- lista cruda de partidos
        matches_clean.csv                <- dim_match (campos DB)
        teams.csv                        <- dim_team  (campos DB)
        players.csv                      <- dim_player (campos DB)
        match_<id>/batch_id=<id>/
            shots.json                   <- tiros crudos
            events.json                  <- incidentes crudos
            lineups.json                 <- alineaciones crudas
            shots_clean.csv              <- fact_shots (campos DB)
            events_clean.csv             <- fact_events (campos DB)

Los loaders/ son los únicos que escriben en la DB.
"""

# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
import os
import json
import re
import sys
import time
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# --------------------------------------------------------------------------- #
# Project paths
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "sofascore"

# --------------------------------------------------------------------------- #
# Logger
# --------------------------------------------------------------------------- #
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
TOURNAMENT_ID = 8                      # La Liga en SofaScore
SEASON_NAMES  = [
    "LaLiga 20/21", "LaLiga 21/22", "LaLiga 22/23",
    "LaLiga 23/24", "LaLiga 24/25", "LaLiga 25/26",
]                                       # temporadas a scrapear
DELAY_SEC     = 0.3                      # pausa entre peticiones
HEADLESS      = False                    # ejecutar Chrome en modo headless

# --------------------------------------------------------------------------- #
# Helpers – Driver
# --------------------------------------------------------------------------- #
def create_driver() -> webdriver.Chrome:
    """
    Crea un driver de Chrome usando Selenium.

    Si se define la variable de entorno ``CHROMEDRIVER_PATH`` se usa ese
    ejecutable; de lo contrario ``webdriver_manager`` descarga el
    driver más reciente compatible con la versión de Chrome.
    """
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.page_load_strategy = "eager"

    driver_path = os.getenv("CHROMEDRIVER_PATH")
    if driver_path:
        service = Service(driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=options)


# --------------------------------------------------------------------------- #
# Helpers – JSON fetch
# --------------------------------------------------------------------------- #
def get_json(driver: webdriver.Chrome, url: str, timeout: float = 2) -> dict:
    """Navega a una URL de la API de SofaScore y devuelve el JSON parseado."""
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(DELAY_SEC)
    return json.loads(driver.find_element("tag name", "body").text)


# --------------------------------------------------------------------------- #
# Helpers – season & matches
# --------------------------------------------------------------------------- #
def get_season_id(
    driver: webdriver.Chrome, tournament_id: int, season_name: str
) -> tuple[Optional[int], Optional[str]]:
    """
    Devuelve (season_id, season_label) para un nombre de temporada dado.

    Consulta el endpoint de temporadas del torneo y busca la que
    contenga ``season_name`` en su nombre.
    """
    data = get_json(
        driver,
        f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons",
    )
    possible_names = {season_name}
    if re.match(r"^\d{4}/\d{4}$", season_name):
        start_year, end_year = season_name.split("/")
        possible_names.add(f"{start_year[-2:]}/{end_year[-2:]}")

    for s in data.get("seasons", []):
        season_label = s.get("name", "")
        if any(name in season_label for name in possible_names):
            return s["id"], season_label
    return None, None


def get_matches(
    driver: webdriver.Chrome, tournament_id: int, season_id: int
) -> list[dict]:
    """
    Devuelve todos los partidos de una temporada paginando el endpoint.

    El endpoint devuelve hasta ~20 partidos por página.
    Se itera hacia atrás hasta agotar las páginas.
    """
    events = []
    page = 0
    while True:
        url = (
            f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}"
            f"/season/{season_id}/events/last/{page}"
        )
        data = get_json(driver, url)
        batch = data.get("events", [])
        if not batch:
            break
        events.extend(batch)
        if not data.get("hasNextPage"):
            break
        page += 1
    return events


def _get_match_date(match: dict) -> Optional[date]:
    """Extrae la fecha de un partido de SofaScore."""
    start_date = match.get("startDate") or match.get("start_date")
    if start_date:
        # "2025-05-25" → YYYY‑MM‑DD
        return date.fromisoformat(start_date[:10])
    timestamp = match.get("timestamp") or match.get("startTime")
    if timestamp:
        return date.fromtimestamp(timestamp)
    return None


# --------------------------------------------------------------------------- #
# Helpers – shot / event / lineup
# --------------------------------------------------------------------------- #
def get_match_shots(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo del mapa de tiros de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/shotmap")


def get_match_events(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de los incidentes de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/incidents")


def get_match_lineups(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de las alineaciones de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/lineups")


# --------------------------------------------------------------------------- #
# Helpers – cache
# --------------------------------------------------------------------------- #
def get_scraped_sofascore_match_ids() -> set[int]:
    """
    Obtiene los id_sofascore de los partidos que ya tienen eventos en la BD.

    Se usa para evitar volver a descargar partidos cuyo ID ya está
    presente en fact_events.
    """
    try:
        from sqlalchemy import text
        from loaders.common import engine

        query = """
            SELECT DISTINCT m.id_sofascore
            FROM dim_match m
            JOIN fact_events e ON m.match_id = e.match_id
            WHERE m.id_sofascore IS NOT NULL
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            return {int(r[0]) for r in rows}
    except Exception as exc:
        log.warning("No se pudo consultar BBDD para cache de SofaScore: %s", exc)
        return set()


# --------------------------------------------------------------------------- #
# Orchestrator – scrape_sofascore
# --------------------------------------------------------------------------- #
def scrape_sofascore(
    season_name: str | None = None,
    tournament_id: int = TOURNAMENT_ID,
    from_date: str | None = None,
    full_refresh: bool = False,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Orquestador principal: obtiene partidos, tiros, eventos y alineaciones.

    Parameters
    ----------
    season_name : str | None
        Nombre de la temporada (ej: "2020/2021"). Si es None, usa la primera de SEASON_NAMES.
    tournament_id : int
        ID del torneo en SofaScore.
    from_date : str | None
        Fecha mínima (YYYY‑MM‑DD). Se filtran solo partidos con fecha >= esta.
    full_refresh : bool
        Si es True, ignora la caché de la BD y descarga todo.

    Returns
    -------
    tuple of 4 lists:
        - matches
        - all_shots
        - all_events
        - all_lineups
    """
    if season_name is None:
        season_name = SEASON_NAMES[0]

    from_date_obj: Optional[date] = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        print(f"\n[FILTER] Descargando solo partidos desde: {from_date}")

    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    driver = create_driver()
    all_shots: list[dict] = []
    all_events: list[dict] = []
    all_lineups: list[dict] = []

    try:
        season_id, season_label = get_season_id(driver, tournament_id, season_name)
        if season_id is None:
            raise ValueError(f"Temporada '{season_name}' no encontrada en SofaScore")

        print(f"\n[SEASON] Temporada: {season_label}  (id={season_id})")
        matches = get_matches(driver, tournament_id, season_id)
        print(f"  [+] {len(matches)} partidos encontrados")

        # Normalizar label para carpetas
        folder_label = season_label.replace("/", "_")
        base_path = OUTPUT_DIR / f"season={folder_label}"
        base_path.mkdir(parents=True, exist_ok=True)

        # Guardar partidos crudos
        _save_json(matches, base_path / f"matches_batch_{batch_id}.json")

        # Filtrar por fecha si se especifica from_date
        if from_date_obj:
            matches = [
                m for m in matches if _get_match_date(m) and _get_match_date(m) >= from_date_obj
            ]
            print(f"  [+] {len(matches)} partidos después de {from_date}")

        # Cache de BD
        scraped_ids = get_scraped_sofascore_match_ids() if not full_refresh else set()
        skipped_matches = 0

        for i, m in enumerate(matches, 1):
            match_id = m["id"]
            home = m.get("homeTeam", {}).get("name", "?")
            away = m.get("awayTeam", {}).get("name", "?")

            if not full_refresh and match_id in scraped_ids:
                skipped_matches += 1
                continue

            print(f"  [{i}/{len(matches)}] Match {match_id}: {home} vs {away}")

            match_dir = base_path / f"match_{match_id}" / f"batch_id={batch_id}"
            match_dir.mkdir(parents=True, exist_ok=True)

            # Tiros
            try:
                shots_raw = get_match_shots(driver, match_id)
                _save_json(shots_raw, match_dir / "shots.json")
                for s in shots_raw.get("shotmap", []):
                    s["_match_id_ss"] = match_id
                    s["_season_label"] = season_label
                    s["_home_team_id_ss"] = m.get("homeTeam", {}).get("id")
                    s["_away_team_id_ss"] = m.get("awayTeam", {}).get("id")
                all_shots.extend(shots_raw.get("shotmap", []))
            except Exception as e:
                log.warning("Shots failed match %d: %s", match_id, e)

            # Eventos
            try:
                events_raw = get_match_events(driver, match_id)
                _save_json(events_raw, match_dir / "events.json")
                for ev in events_raw.get("incidents", []):
                    ev["_match_id_ss"] = match_id
                    ev["_season_label"] = season_label
                all_events.extend(events_raw.get("incidents", []))
            except Exception as e:
                log.warning("Events failed match %d: %s", match_id, e)

            # Alineaciones
            try:
                lineups_raw = get_match_lineups(driver, match_id)
                _save_json(lineups_raw, match_dir / "lineups.json")
                all_lineups.append({"match_id": match_id, "data": lineups_raw})
            except Exception as e:
                log.warning("Fallo general en partido %d: %s", match_id, e)

    finally:
        driver.quit()

    if not full_refresh:
        print(f"\n  [INFO] Partidos omitidos (ya en BD): {skipped_matches}")

    if matches:
        df_matches = transform_matches(matches)
        df_shots = transform_shots(all_shots)
        df_events = transform_events(all_events)
        df_teams = extract_teams(matches)
        df_players = extract_players(df_shots, df_events)

        season_dir = OUTPUT_DIR / f"season={season_name.replace('/', '_')}"
        season_dir.mkdir(parents=True, exist_ok=True)

        df_matches.to_csv(season_dir / "matches_clean.csv", index=False, encoding="utf-8-sig")
        df_shots.to_csv(season_dir / "shots_clean.csv", index=False, encoding="utf-8-sig")
        df_events.to_csv(season_dir / "events_clean.csv", index=False, encoding="utf-8-sig")
        df_teams.to_csv(season_dir / "teams.csv", index=False, encoding="utf-8-sig")
        df_players.to_csv(season_dir / "players.csv", index=False, encoding="utf-8-sig")

    return matches, all_shots, all_events, all_lineups


# --------------------------------------------------------------------------- #
# Transform functions
# --------------------------------------------------------------------------- #
def _ss_timestamp_to_date(ts: int | None) -> Optional[str]:
    """Convierte un Unix timestamp de SofaScore a cadena YYYY‑MM‑DD."""
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=datetime.utc).strftime("%Y-%m-%d")


def transform_matches(matches: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de partidos a las columnas de dim_match."""
    rows = []
    for m in matches:
        rows.append(
            {
                "id_sofascore": m.get("id"),
                "match_date": _ss_timestamp_to_date(m.get("startTimestamp")),
                "competition": m.get("tournament", {}).get("name"),
                "season": m.get("season", {}).get("name"),
                "home_team_id_ss": m.get("homeTeam", {}).get("id"),
                "away_team_id_ss": m.get("awayTeam", {}).get("id"),
                "home_team_name": m.get("homeTeam", {}).get("name"),
                "away_team_name": m.get("awayTeam", {}).get("name"),
                "home_score": m.get("homeScore", {}).get("current"),
                "away_score": m.get("awayScore", {}).get("current"),
                "data_source": "sofascore",
            }
        )
    return pd.DataFrame(rows)


def transform_shots(shots_raw: list[dict]) -> pd.DataFrame:
    """Adapta los tiros crudos a las columnas de fact_shots."""
    rows = []
    for s in shots_raw:
        player = s.get("player", {})
        rows.append(
            {
                "match_id_ss": s.get("_match_id_ss"),
                "player_id_ss": player.get("id"),
                "player_name": player.get("name"),
                "team_id_ss": s.get("teamId"),
                "minute": s.get("time"),
                "x": s.get("playerCoordinates", {}).get("x"),
                "y": s.get("playerCoordinates", {}).get("y"),
                "xg": s.get("xg"),
                "result": s.get("shotType"),
                "shot_type": s.get("bodyPart"),
                "situation": s.get("situation"),
                "data_source": "sofascore",
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["x"] = pd.to_numeric(df["x"], errors="coerce").round(4)
        df["y"] = pd.to_numeric(df["y"], errors="coerce").round(4)
        df["xg"] = pd.to_numeric(df["xg"], errors="coerce").round(4)
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
    return df


def transform_events(events_raw: list[dict]) -> pd.DataFrame:
    """Adapta los incidentes crudos a las columnas de fact_events."""
    rows = []
    for ev in events_raw:
        player = ev.get("player", {})
        point = ev.get("incidentPoint") or {}
        rows.append(
            {
                "match_id_ss": ev.get("_match_id_ss"),
                "player_id_ss": player.get("id"),
                "player_name": player.get("name"),
                "team_id_ss": ev.get("teamId"),
                "event_type": ev.get("incidentType"),
                "minute": ev.get("time"),
                "second": None,
                "x": point.get("x"),
                "y": point.get("y"),
                "end_x": None,
                "end_y": None,
                "outcome": ev.get("incidentClass"),
                "data_source": "sofascore",
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
        df["x"] = pd.to_numeric(df["x"], errors="coerce").round(4)
        df["y"] = pd.to_numeric(df["y"], errors="coerce").round(4)
    return df


def extract_teams(matches: list[dict]) -> pd.DataFrame:
    """Extrae equipos únicos de la lista de partidos → columnas de dim_team."""
    teams = {}
    for m in matches:
        for side in ("homeTeam", "awayTeam"):
            t = m.get(side, {})
            tid = t.get("id")
            if tid and tid not in teams:
                teams[tid] = t.get("name")
    df = pd.DataFrame(
        [{"id_sofascore": k, "canonical_name": v} for k, v in teams.items()]
    ).sort_values("id_sofascore").reset_index(drop=True)
    return df


def extract_players(shots_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """Extrae jugadores únicos de tiros y eventos → columnas de dim_player."""
    frames = []
    for df in (shots_df, events_df):
        if not df.empty and "player_id_ss" in df.columns:
            frames.append(
                df[["player_id_ss", "player_name"]]
                .rename(
                    columns={
                        "player_id_ss": "id_sofascore",
                        "player_name": "canonical_name",
                    }
                )
            )
    if not frames:
        return pd.DataFrame(columns=["id_sofascore", "canonical_name"])
    return (
        pd.concat(frames)
        .drop_duplicates(subset=["id_sofascore"])
        .dropna(subset=["id_sofascore"])
        .sort_values("id_sofascore")
        .reset_index(drop=True)
    )


# --------------------------------------------------------------------------- #
# Helper – JSON persistence
# --------------------------------------------------------------------------- #
def _save_json(data: object, path: Path) -> None:
    """Guarda JSON en disco de forma segura."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# --------------------------------------------------------------------------- #
# Main entry point – scrape a single season (used by the wizard)
# --------------------------------------------------------------------------- #
def main() -> None:
    """Script ejecutable: permite lanzar el scraper desde la línea de comandos."""
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    print("=" * 55)
    print(f"  SofaScore scraper - La Liga {SEASON_NAMES[0]} a {SEASON_NAMES[-1]}")
    print("=" * 55)

    parser = argparse.ArgumentParser(description="Scraper de SofaScore")
    parser.add_argument(
        "--tournament-id",
        "-t",
        type=int,
        default=None,
        help="ID del torneo en SofaScore (ej: 8 para La Liga)",
    )
    parser.add_argument(
        "--season",
        "-s",
        type=str,
        default=None,
        help="Temporada a scrapear (ej: 2024/2025)",
    )

    args = parser.parse_args()

    tournament_id = args.tournament_id if args.tournament_id else TOURNAMENT_ID
    season_name = args.season if args.season else SEASON_NAMES[0]

    # Ejecuta la función principal del scraper
    scrape_sofascore(season_name=season_name, tournament_id=tournament_id)


if __name__ == "__main__":
    main()
