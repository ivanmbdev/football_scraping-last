#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
wizard.py
=========
Interactive wizard that orchestrates the whole football‑data ETL.

Features
--------
* Full season scrape or incremental update
* Competition selection (national / continental / inter‑continental)
* Season selection
* Source selection (understat, sofascore, transfermarkt, statsbomb, whoscored)
* Optional match filtering:
    • All matches
    • Matches of a single team (after scraping)
    • Matches from a specific start date
* CSV export of the matches for a chosen team

The wizard uses the existing scrapers, loaders and helper functions that live in the
repository – no new scraping logic is added.

Usage
-----
Interactive:
    $ python -m scripts.wizard

Command‑line (no prompts):
    $ python -m scripts.wizard --competition "La Liga" --season 2024/2025 --scrape
    $ python -m scripts.wizard --competition "Champions League" --update
    $ python -m scripts.wizard --competition "La Liga" --season 2024/2025 --scrape --team "real-madrid"
"""

# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
import argparse
import datetime
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Core modules from the repo
from scripts.pipeline_runner import (
    run_pipeline,
    list_available_competitions,          # helper used only for CLI help
    get_available_seasons,
    get_last_match_date,
    get_current_season,
)
from scripts.competitions import COMPETITIONS, get_competition, get_season_start_year
from scrapers.transfermarkt_scraper import get_league_teams
from loaders.common import engine
from sqlalchemy import text

# --------------------------------------------------------------------------- #
# Helpers – interactive prompts
# --------------------------------------------------------------------------- #
def prompt_choice(prompt: str, options: List[str], default: Optional[str] = None) -> str:
    """Show a numbered menu and return the chosen option."""
    if default:
        print(f"{prompt} (default: {default})")
    else:
        print(prompt)
    for i, opt in enumerate(options, 1):
        print(f"  {i}) {opt}")
    while True:
        choice = input("Selecciona una opción: ").strip()
        if not choice and default:
            return default
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]
        print("Entrada inválida, prueba de nuevo.")


def prompt_date(prompt: str, default: Optional[str] = None) -> Optional[str]:
    """Ask for a date in YYYY-MM-DD format (or empty)."""
    if default:
        print(f"{prompt} (default: {default})")
    else:
        print(prompt)
    while True:
        d = input("Introduce la fecha (YYYY-MM-DD) o pulsa ENTER para omitir: ").strip()
        if not d:
            return None
        try:
            datetime.datetime.strptime(d, "%Y-%m-%d")
            return d
        except ValueError:
            print("Formato inválido, prueba de nuevo.")


# --------------------------------------------------------------------------- #
# Selection helpers
# --------------------------------------------------------------------------- #
def choose_competition() -> str:
    """Ask the user to choose a competition."""
    # We display the raw COMPETITIONS keys – they are already grouped nicely
    options = list(COMPETITIONS.keys())
    comp_name = prompt_choice("Selecciona la competición:", options)
    return comp_name


def choose_season() -> str:
    """Ask the user to choose a season."""
    # Show a realistic range of seasons
    seasons = get_available_seasons(start_year=2018, end_year=2026)
    season = prompt_choice("Selecciona la temporada a procesar:", seasons)
    return season


def choose_source() -> str:
    """Ask the user to choose the data source(s)."""
    options = ["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"]
    src = prompt_choice("Selecciona la fuente(s) de datos a usar:", options, default="all")
    return src


def choose_match_filter(comp_conf: Dict[str, any], season_start: int) -> Dict[str, Optional[str]]:
    """
    Ask how the user wants to filter the matches after scraping.

    Returns a dict:
        match_type : "all" | "team" | "date"
        team_slug : if match_type=="team"
        from_date : if match_type=="date"
    """
    print("\nSelecciona cómo filtrar los partidos descargados:")
    match_type = prompt_choice(
        "  • Todos los partidos",
        ["All", "Team", "Date"],
        default="All",
    ).lower()

    result: Dict[str, Optional[str]] = {"match_type": match_type, "team_slug": None, "from_date": None}

    if match_type == "team":
        league_code = comp_conf["sources"]["transfermarkt"]["league_code"]
        teams_dict = get_league_teams(league_code, str(season_start))
        if not teams_dict:
            print("  [ERROR] No se pudieron obtener equipos.")
            result["match_type"] = "all"
            return result
        team_slugs = list(teams_dict.keys())
        team_slug = prompt_choice("Selecciona el equipo", team_slugs)
        result["team_slug"] = team_slug

    elif match_type == "date":
        d = prompt_date("Introduce la fecha de inicio (YYYY-MM-DD)")
        result["from_date"] = d

    return result


# --------------------------------------------------------------------------- #
# Export helper (team‑specific)
# --------------------------------------------------------------------------- #
def export_matches_for_team(team_slug: str, competition: str, season: str) -> None:
    """
    After a full scrape, export a CSV that contains only the matches of a
    single team.

    The file is written to data/exports/<competition>_<season>_team_<team_slug>.csv
    """
    print(f"\n[EXPORT] Generando CSV con los partidos de {team_slug}...")

    # Try to resolve the team in dim_team.  We first try a LIKE query on
    # canonical_name (replacing hyphens with spaces) – this works for most
    # cases (e.g. "real-madrid" → "real madrid").
    with engine.connect() as conn:
        like_pattern = f"%{team_slug.replace('-', ' ')}%"
        row = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) LIKE :like LIMIT 1"),
            {"like": like_pattern.lower()},
        ).fetchone()
        if not row:
            print("  [ERROR] No se encontró el equipo en dim_team.")
            return
        team_cid = row[0]

        # Pull all matches that involve this team in the chosen season
        matches = conn.execute(
            text(
                """
                SELECT
                    m.match_id,
                    m.match_date,
                    m.competition,
                    m.season,
                    m.home_team_id,
                    m.away_team_id,
                    m.home_score,
                    m.away_score,
                    m.data_source,
                    m.id_sofascore,
                    m.id_understat,
                    m.id_statsbomb,
                    m.id_whoscored
                FROM dim_match m
                WHERE (m.home_team_id = :tid OR m.away_team_id = :tid)
                  AND m.season = :season
                ORDER BY m.match_date
                """
            ),
            {"tid": team_cid, "season": season},
        ).fetchall()

        if not matches:
            print("  [INFO] No hay partidos para ese equipo en la temporada seleccionada.")
            return

        # Write CSV
        export_dir = Path("data") / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        out_path = export_dir / f"{competition}_{season}_team_{team_slug}.csv"

        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "match_id",
                    "match_date",
                    "competition",
                    "season",
                    "home_team_id",
                    "away_team_id",
                    "home_score",
                    "away_score",
                    "data_source",
                    "id_sofascore",
                    "id_understat",
                    "id_statsbomb",
                    "id_whoscored",
                ]
            )
            for r in matches:
                writer.writerow(r)

        print(f"  [OK] CSV creado en {out_path}")


# --------------------------------------------------------------------------- #
# Interactive flow
# --------------------------------------------------------------------------- #
def interactive_flow() -> None:
    """Run the wizard in interactive mode."""
    print("\n=== FOOTBALL DATA PIPELINE WIZARD ===")

    # 1) Operation type
    op = prompt_choice(
        "¿Qué quieres hacer?",
        ["Descargar temporada completa", "Actualizar datos con juegos nuevos"],
        default="Descargar temporada completa",
    )
    full_scrape = op.lower().startswith("descargar")

    # 2) Competition
    competition = choose_competition()
    comp_conf = get_competition(competition)
    if not comp_conf:
        print("  [ERROR] Competición no encontrada.")
        sys.exit(1)

    # 3) Season
    season = choose_season()
    season_start = get_season_start_year(season)

    # 4) Source(s)
    source = choose_source()

    # 5) Match filtering
    match_filter = choose_match_filter(comp_conf, season_start)
    from_date = match_filter.get("from_date")
    team_slug = match_filter.get("team_slug")

    # 6) Build arguments for the pipeline
    kwargs = {
        "scrape": full_scrape,
        "competition": competition,
        "source": source,
        "season": season,
        "from_date": from_date,
        "update": not full_scrape,
    }

    print("\n=== INICIANDO EL PROCESO ===")
    run_pipeline(**kwargs)

    # 7) Export matches for a single team (if chosen)
    if team_slug:
        export_matches_for_team(
            team_slug,
            competition.replace(" ", "_"),
            season,
        )

    print("\n=== PROCESO FINALIZADO EXITOSAMENTE ===")


# --------------------------------------------------------------------------- #
# CLI entry point
# --------------------------------------------------------------------------- #
def parse_cli_args() -> argparse.Namespace:
    """Parse command‑line arguments for non‑interactive usage."""
    parser = argparse.ArgumentParser(
        description="Football data wizard – interactive or CLI",
        epilog="""
Si no pasas ningún argumento, la consola se pondrá en modo interactivo.
Los argumentos que aceptamos son una capa de configuración sobre run_pipeline().
""",
    )

    parser.add_argument(
        "--competition",
        help="Nombre de la competición (ej. 'La Liga')",
    )
    parser.add_argument(
        "--season",
        help="Temporada a procesar (ej. 2024/2025)",
    )
    parser.add_argument(
        "--source",
        choices=["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"],
        default="all",
        help="Fuente(s) de datos a usar (por defecto: all)",
    )
    parser.add_argument(
        "--scrape",
        action="store_true",
        help="Forzar un scrape completo (ignora la BD)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Realiza un update incremental (usa la última fecha de la BD)",
    )
    parser.add_argument(
        "--from-date",
        help="Fecha mínima en formato YYYY-MM-DD (para filtros de fechas)",
    )
    parser.add_argument(
        "--team",
        help="Slug del equipo para exportar los partidos (ej. 'real-madrid')",
    )

    return parser.parse_args()


def main() -> None:
    """Entrypoint – either interactive or CLI based on supplied arguments."""
    args = parse_cli_args()

    # If the user did not supply any argument, go interactive
    if not any(
        [
            args.competition,
            args.season,
            args.source != "all",
            args.scrape,
            args.update,
            args.from_date,
            args.team,
        ]
    ):
        interactive_flow()
        return

    # --------------------  CLI mode  -------------------- #
    # Validate competition
    competition = args.competition
    if not competition:
        print("ERROR: debes especificar --competition")
        sys.exit(1)
    comp_conf = get_competition(competition)
    if not comp_conf:
        print(f"ERROR: la competición '{competition}' no existe.")
        sys.exit(1)

    season = args.season or get_current_season()
    season_start = get_season_start_year(season)

    kwargs = {
        "scrape": args.scrape or not args.update,
        "competition": competition,
        "source": args.source,
        "season": season,
        "from_date": args.from_date,
        "update": args.update,
    }

    print("\n=== INICIANDO EL PROCESO (CLI) ===")
    run_pipeline(**kwargs)

    # Export for a single team if requested
    if args.team:
        export_matches_for_team(
            args.team,
            competition.replace(" ", "_"),
            season,
        )

    print("\n=== PROCESO FINALIZADO EXITOSAMENTE ===")


if __name__ == "__main__":
    main()
