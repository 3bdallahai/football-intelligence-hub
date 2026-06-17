"""
La Liga 2026-2027 Season Data Generator
========================================
Mimics the structure of the historical La Liga dataset (player CSVs + transfer market values).
Generates realistic random data for the 2026-2027 season that can be fed into your pipeline.

OUTPUT MODES
------------
1. player_stats      → one CSV per player per team (mirrors La Liga zip structure)
2. transfer_values   → one CSV with market value snapshots across the season
3. match_results     → La Liga 2026-2027 match schedule + results
4. all               → generate everything

USAGE
-----
    python la_liga_2026_27_generator.py --mode all --output ./generated_data
    python la_liga_2026_27_generator.py --mode player_stats --output ./generated_data
    python la_liga_2026_27_generator.py --mode transfer_values --output ./generated_data
    python la_liga_2026_27_generator.py --mode match_results --output ./generated_data

    # Stream a single match result (API/scraper simulation):
    python la_liga_2026_27_generator.py --mode match_results --stream --matchday 1
"""

import argparse
import json
import math
import os
import random
import time
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# SEED DATA  (derived from real 2025-2026 squads)
# ─────────────────────────────────────────────────────────────────────────────

SEASON = "2026-2027"
COMPETITION = "La Liga"

LA_LIGA_TEAMS = [
    "Alavés", "Athletic Club", "Atlético Madrid", "Barcelona", "Celta Vigo",
    "Espanyol", "Getafe", "Girona", "Leganés", "Mallorca",
    "Osasuna", "Rayo Vallecano", "Real Betis", "Real Madrid", "Real Sociedad",
    "Sevilla", "Valencia", "Villarreal", "Real Valladolid", "Las Palmas",
]

# Approximate mid-season squad market cap (€M) — drives market value generation
TEAM_BUDGET = {
    "Real Madrid":     900, "Barcelona":       750, "Atlético Madrid": 600,
    "Athletic Club":   280, "Real Sociedad":   320, "Real Betis":       280,
    "Villarreal":      260, "Sevilla":         220, "Girona":           220,
    "Celta Vigo":      160, "Osasuna":         130, "Rayo Vallecano":   110,
    "Mallorca":        120, "Getafe":          100, "Espanyol":         160,
    "Alavés":          100, "Leganés":          90, "Valencia":         200,
    "Real Valladolid":  80, "Las Palmas":       90,
}

# Real player pools per team (from 2025-2026) + fictional additions for 2026-2027
SQUADS: dict[str, list[dict]] = {
    "Real Madrid": [
        {"name": "Thibaut Courtois",  "pos": "GK",  "age": 34, "nationality": "Belgium",   "foot": "Right"},
        {"name": "Andriy Lunin",      "pos": "GK",  "age": 26, "nationality": "Ukraine",    "foot": "Right"},
        {"name": "Éder Militão",      "pos": "DF",  "age": 27, "nationality": "Brazil",     "foot": "Right"},
        {"name": "David Alaba",       "pos": "DF",  "age": 34, "nationality": "Austria",    "foot": "Left"},
        {"name": "Antonio Rüdiger",   "pos": "DF",  "age": 33, "nationality": "Germany",    "foot": "Right"},
        {"name": "Ferland Mendy",     "pos": "DF",  "age": 30, "nationality": "France",     "foot": "Left"},
        {"name": "Lucas Vázquez",     "pos": "DF",  "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Raúl Asencio",      "pos": "DF",  "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "Dani Carvajal",     "pos": "DF",  "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Luka Modrić",       "pos": "MF",  "age": 40, "nationality": "Croatia",    "foot": "Right"},
        {"name": "Aurélien Tchouaméni", "pos": "MF", "age": 26, "nationality": "France",   "foot": "Right"},
        {"name": "Federico Valverde", "pos": "MF",  "age": 26, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Eduardo Camavinga", "pos": "MF",  "age": 23, "nationality": "France",     "foot": "Left"},
        {"name": "Jude Bellingham",   "pos": "MF",  "age": 23, "nationality": "England",    "foot": "Right"},
        {"name": "Dani Ceballos",     "pos": "MF",  "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Kylian Mbappé",     "pos": "FW",  "age": 27, "nationality": "France",     "foot": "Right"},
        {"name": "Vinícius Jr.",      "pos": "FW",  "age": 26, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Rodrygo",           "pos": "FW",  "age": 25, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Endrick",           "pos": "FW",  "age": 19, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Brahim Díaz",       "pos": "FW",  "age": 26, "nationality": "Morocco",    "foot": "Right"},
    ],
    "Barcelona": [
        {"name": "Iñaki Peña",         "pos": "GK", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Wojciech Szczęsny",  "pos": "GK", "age": 35, "nationality": "Poland",     "foot": "Right"},
        {"name": "Ronald Araújo",      "pos": "DF", "age": 26, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Pau Cubarsí",        "pos": "DF", "age": 18, "nationality": "Spain",      "foot": "Right"},
        {"name": "Alejandro Balde",    "pos": "DF", "age": 22, "nationality": "Spain",      "foot": "Left"},
        {"name": "Jules Koundé",       "pos": "DF", "age": 26, "nationality": "France",     "foot": "Right"},
        {"name": "Pedri",              "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Left"},
        {"name": "Frenkie de Jong",    "pos": "MF", "age": 29, "nationality": "Netherlands","foot": "Right"},
        {"name": "Gavi",               "pos": "MF", "age": 22, "nationality": "Spain",      "foot": "Left"},
        {"name": "Dani Olmo",          "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Marc Casadó",        "pos": "MF", "age": 21, "nationality": "Spain",      "foot": "Right"},
        {"name": "Lamine Yamal",       "pos": "FW", "age": 19, "nationality": "Spain",      "foot": "Right"},
        {"name": "Raphinha",           "pos": "FW", "age": 29, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Robert Lewandowski", "pos": "FW", "age": 38, "nationality": "Poland",     "foot": "Right"},
        {"name": "Ferran Torres",      "pos": "FW", "age": 25, "nationality": "Spain",      "foot": "Left"},
        {"name": "Ansu Fati",          "pos": "FW", "age": 23, "nationality": "Spain",      "foot": "Left"},
    ],
    "Atlético Madrid": [
        {"name": "Jan Oblak",          "pos": "GK", "age": 32, "nationality": "Slovenia",   "foot": "Right"},
        {"name": "José Giménez",       "pos": "DF", "age": 30, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Marcos Llorente",    "pos": "DF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Nahuel Molina",      "pos": "DF", "age": 27, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Reinildo",           "pos": "DF", "age": 31, "nationality": "Mozambique", "foot": "Left"},
        {"name": "Robin Le Normand",   "pos": "DF", "age": 28, "nationality": "France",     "foot": "Right"},
        {"name": "Koke",               "pos": "MF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Rodrigo De Paul",    "pos": "MF", "age": 31, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Saúl Ñíguez",        "pos": "MF", "age": 32, "nationality": "Spain",      "foot": "Left"},
        {"name": "Conor Gallagher",    "pos": "MF", "age": 25, "nationality": "England",    "foot": "Right"},
        {"name": "Pablo Barrios",      "pos": "MF", "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "Antoine Griezmann",  "pos": "FW", "age": 35, "nationality": "France",     "foot": "Left"},
        {"name": "Álvaro Morata",      "pos": "FW", "age": 34, "nationality": "Spain",      "foot": "Right"},
        {"name": "Samuel Lino",        "pos": "FW", "age": 25, "nationality": "Portugal",   "foot": "Left"},
        {"name": "Julián Álvarez",     "pos": "FW", "age": 25, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Alexander Sørloth",  "pos": "FW", "age": 30, "nationality": "Norway",     "foot": "Right"},
    ],
    "Athletic Club": [
        {"name": "Unai Simón",         "pos": "GK", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Aitor Paredes",      "pos": "DF", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Dani Vivian",        "pos": "DF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Yuri Berchiche",     "pos": "DF", "age": 35, "nationality": "Spain",      "foot": "Left"},
        {"name": "Óscar de Marcos",    "pos": "DF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Andoni Gorosabel",   "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Mikel Vesga",        "pos": "MF", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Oihan Sancet",       "pos": "MF", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "Nico Williams",      "pos": "FW", "age": 23, "nationality": "Spain",      "foot": "Left"},
        {"name": "Iñaki Williams",     "pos": "FW", "age": 31, "nationality": "Ghana",      "foot": "Right"},
        {"name": "Gorka Guruzeta",     "pos": "FW", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Beñat Prados",       "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Right"},
    ],
    "Real Sociedad": [
        {"name": "Álex Remiro",        "pos": "GK", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Aritz Elustondo",    "pos": "DF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Igor Zubeldia",      "pos": "DF", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Hamari Traoré",      "pos": "DF", "age": 33, "nationality": "Mali",       "foot": "Right"},
        {"name": "Aihen Muñoz",        "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Left"},
        {"name": "Mikel Merino",       "pos": "MF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Brais Méndez",       "pos": "MF", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Martín Zubimendi",   "pos": "MF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Mikel Oyarzabal",    "pos": "FW", "age": 28, "nationality": "Spain",      "foot": "Left"},
        {"name": "Sheraldo Becker",    "pos": "FW", "age": 29, "nationality": "Suriname",   "foot": "Right"},
        {"name": "Ander Barrenetxea",  "pos": "FW", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Take Kubo",          "pos": "FW", "age": 24, "nationality": "Japan",      "foot": "Right"},
    ],
    "Real Betis": [
        {"name": "Rui Silva",          "pos": "GK", "age": 31, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Germán Pezzella",    "pos": "DF", "age": 33, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Natan",              "pos": "DF", "age": 24, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Ricardo Rodríguez",  "pos": "DF", "age": 32, "nationality": "Switzerland","foot": "Left"},
        {"name": "Youssouf Sabaly",    "pos": "DF", "age": 32, "nationality": "Senegal",    "foot": "Right"},
        {"name": "Guido Rodríguez",    "pos": "MF", "age": 31, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Isco",               "pos": "MF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Sergi Altimira",     "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Marc Roca",          "pos": "MF", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Abde Ezzalzouli",    "pos": "FW", "age": 23, "nationality": "Morocco",    "foot": "Right"},
        {"name": "Antony",             "pos": "FW", "age": 26, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Vitor Roque",        "pos": "FW", "age": 20, "nationality": "Brazil",     "foot": "Right"},
    ],
    "Villarreal": [
        {"name": "Filip Jörgensen",    "pos": "GK", "age": 24, "nationality": "Denmark",    "foot": "Right"},
        {"name": "Juan Foyth",         "pos": "DF", "age": 27, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Pau Torres",         "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Alfonso Pedraza",    "pos": "DF", "age": 28, "nationality": "Spain",      "foot": "Left"},
        {"name": "Jorge Cuenca",       "pos": "DF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Dani Parejo",        "pos": "MF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Étienne Capoue",     "pos": "MF", "age": 37, "nationality": "France",     "foot": "Right"},
        {"name": "Alex Baena",         "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Yeremy Pino",        "pos": "FW", "age": 23, "nationality": "Spain",      "foot": "Right"},
        {"name": "Samu Chukwueze",     "pos": "FW", "age": 26, "nationality": "Nigeria",    "foot": "Right"},
        {"name": "Nicolas Jackson",    "pos": "FW", "age": 24, "nationality": "Senegal",    "foot": "Right"},
        {"name": "Ayoze Pérez",        "pos": "FW", "age": 32, "nationality": "Spain",      "foot": "Right"},
    ],
    "Sevilla": [
        {"name": "Álvaro Fernández",   "pos": "GK", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Marcos Acuña",       "pos": "DF", "age": 33, "nationality": "Argentina",  "foot": "Left"},
        {"name": "Loïc Badé",          "pos": "DF", "age": 25, "nationality": "France",     "foot": "Right"},
        {"name": "José Ángel",         "pos": "DF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Kike Salas",         "pos": "DF", "age": 23, "nationality": "Spain",      "foot": "Right"},
        {"name": "Lucien Agoumé",      "pos": "MF", "age": 24, "nationality": "France",     "foot": "Right"},
        {"name": "Djibril Sow",        "pos": "MF", "age": 28, "nationality": "Switzerland","foot": "Right"},
        {"name": "Joan Jordán",        "pos": "MF", "age": 31, "nationality": "Spain",      "foot": "Right"},
        {"name": "Juanlu Sánchez",     "pos": "FW", "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "Chidera Ejuke",      "pos": "FW", "age": 27, "nationality": "Nigeria",    "foot": "Left"},
        {"name": "Dodi Lukebakio",     "pos": "FW", "age": 28, "nationality": "Belgium",    "foot": "Right"},
        {"name": "Isaac Romero",       "pos": "FW", "age": 24, "nationality": "Spain",      "foot": "Right"},
    ],
    "Celta Vigo": [
        {"name": "Iván Villar",        "pos": "GK", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Unai Núñez",         "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Óscar Mingueza",     "pos": "DF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Carlos Domínguez",   "pos": "DF", "age": 25, "nationality": "Spain",      "foot": "Left"},
        {"name": "Hugo Álvarez",       "pos": "MF", "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "Fran Beltrán",       "pos": "MF", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Iago Aspas",         "pos": "FW", "age": 37, "nationality": "Spain",      "foot": "Left"},
        {"name": "Jorgen Strand Larsen","pos": "FW", "age": 25, "nationality": "Norway",    "foot": "Right"},
        {"name": "Jonathan Bamba",     "pos": "FW", "age": 29, "nationality": "France",     "foot": "Left"},
        {"name": "Williot Swedberg",   "pos": "FW", "age": 22, "nationality": "Sweden",     "foot": "Right"},
        {"name": "Anastasios Douvikas","pos": "FW", "age": 26, "nationality": "Greece",     "foot": "Right"},
        {"name": "Marcos Alonso",      "pos": "DF", "age": 34, "nationality": "Spain",      "foot": "Left"},
    ],
    "Osasuna": [
        {"name": "Sergio Herrera",     "pos": "GK", "age": 31, "nationality": "Spain",      "foot": "Right"},
        {"name": "David García",       "pos": "DF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Aridane Hernández",  "pos": "DF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Juan Cruz",          "pos": "DF", "age": 26, "nationality": "Argentina",  "foot": "Left"},
        {"name": "Moi Gómez",          "pos": "MF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Lucas Torró",        "pos": "MF", "age": 31, "nationality": "Spain",      "foot": "Right"},
        {"name": "Jon Moncayola",      "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Abde Rebbach",       "pos": "FW", "age": 23, "nationality": "Morocco",    "foot": "Left"},
        {"name": "Ante Budimir",       "pos": "FW", "age": 34, "nationality": "Croatia",    "foot": "Right"},
        {"name": "Pablo Ibáñez",       "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Rubén García",       "pos": "FW", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Kike Barja",         "pos": "FW", "age": 28, "nationality": "Spain",      "foot": "Right"},
    ],
    "Mallorca": [
        {"name": "Predrag Rajković",   "pos": "GK", "age": 30, "nationality": "Serbia",     "foot": "Right"},
        {"name": "Pablo Maffeo",       "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Antonio Raillo",     "pos": "DF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Jaume Costa",        "pos": "DF", "age": 37, "nationality": "Spain",      "foot": "Left"},
        {"name": "Brian Olivan",       "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Left"},
        {"name": "Dani Rodríguez",     "pos": "MF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Samu Costa",         "pos": "MF", "age": 24, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Dominic Greaves",    "pos": "MF", "age": 24, "nationality": "England",    "foot": "Right"},
        {"name": "Vedat Muriqi",       "pos": "FW", "age": 31, "nationality": "Kosovo",     "foot": "Right"},
        {"name": "Lee Kang-in",        "pos": "FW", "age": 24, "nationality": "South Korea","foot": "Right"},
        {"name": "Cyle Larin",         "pos": "FW", "age": 30, "nationality": "Canada",     "foot": "Right"},
        {"name": "Larin Blanco",       "pos": "FW", "age": 22, "nationality": "Spain",      "foot": "Right"},
    ],
    "Girona": [
        {"name": "Paulo Gazzaniga",    "pos": "GK", "age": 33, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Yan Couto",          "pos": "DF", "age": 23, "nationality": "Brazil",     "foot": "Right"},
        {"name": "David López",        "pos": "DF", "age": 34, "nationality": "Spain",      "foot": "Right"},
        {"name": "Miguel Gutiérrez",   "pos": "DF", "age": 23, "nationality": "Spain",      "foot": "Left"},
        {"name": "Blind",              "pos": "DF", "age": 35, "nationality": "Netherlands","foot": "Left"},
        {"name": "Iván Martín",        "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Oriol Romeu",        "pos": "MF", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Donny van de Beek",  "pos": "MF", "age": 29, "nationality": "Netherlands","foot": "Right"},
        {"name": "Sávio",              "pos": "FW", "age": 21, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Artem Dovbyk",       "pos": "FW", "age": 28, "nationality": "Ukraine",    "foot": "Right"},
        {"name": "Bryan Gil",          "pos": "FW", "age": 24, "nationality": "Spain",      "foot": "Left"},
        {"name": "Yangel Herrera",     "pos": "MF", "age": 27, "nationality": "Venezuela",  "foot": "Right"},
    ],
    "Espanyol": [
        {"name": "Joan García",        "pos": "GK", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Carlos Romero",      "pos": "DF", "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "Leandro Cabrera",    "pos": "DF", "age": 35, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Pol Lozano",         "pos": "DF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Aleix Vidal",        "pos": "DF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Mikel Merino",       "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Sergi Darder",       "pos": "MF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Luis Baldé",         "pos": "FW", "age": 24, "nationality": "Spain",      "foot": "Left"},
        {"name": "Javi Puado",         "pos": "FW", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "Romario Ibarra",     "pos": "FW", "age": 30, "nationality": "Ecuador",    "foot": "Right"},
        {"name": "Edu Expósito",       "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Rubén Sánchez",      "pos": "FW", "age": 22, "nationality": "Spain",      "foot": "Right"},
    ],
    "Getafe": [
        {"name": "David Soria",        "pos": "GK", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Stefan Mitrović",    "pos": "DF", "age": 34, "nationality": "Serbia",     "foot": "Right"},
        {"name": "Domingos Duarte",    "pos": "DF", "age": 30, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Gastón Álvarez",     "pos": "DF", "age": 24, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Killian Sildillia",  "pos": "DF", "age": 23, "nationality": "France",     "foot": "Right"},
        {"name": "Carles Aleñá",       "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Mauro Arambarri",    "pos": "MF", "age": 29, "nationality": "Uruguay",    "foot": "Right"},
        {"name": "Diego Rico",         "pos": "DF", "age": 31, "nationality": "Spain",      "foot": "Left"},
        {"name": "Óscar Rodríguez",    "pos": "FW", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Borja Mayoral",      "pos": "FW", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Christantus Uche",   "pos": "FW", "age": 26, "nationality": "Nigeria",    "foot": "Right"},
        {"name": "Álvaro Rodríguez",   "pos": "FW", "age": 22, "nationality": "Spain",      "foot": "Right"},
    ],
    "Alavés": [
        {"name": "Antonio Sivera",     "pos": "GK", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Martin Aguirregabiria","pos":"DF", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Moussa Diarra",      "pos": "DF", "age": 26, "nationality": "Mali",       "foot": "Right"},
        {"name": "Abdelkabir Abqar",   "pos": "DF", "age": 24, "nationality": "Morocco",    "foot": "Right"},
        {"name": "Jon Guridi",         "pos": "MF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Ander Guevara",      "pos": "MF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Denis Suárez",       "pos": "MF", "age": 31, "nationality": "Spain",      "foot": "Left"},
        {"name": "Carlos Vicente",     "pos": "FW", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Toni Martínez",      "pos": "FW", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Nikola Maraš",       "pos": "FW", "age": 31, "nationality": "Serbia",     "foot": "Right"},
        {"name": "Calebe",             "pos": "FW", "age": 22, "nationality": "Brazil",     "foot": "Right"},
        {"name": "Jonny Castro",       "pos": "DF", "age": 30, "nationality": "Spain",      "foot": "Right"},
    ],
    "Rayo Vallecano": [
        {"name": "Stole Dimitrievski", "pos": "GK", "age": 33, "nationality": "North Macedonia","foot": "Right"},
        {"name": "Fran García",        "pos": "DF", "age": 25, "nationality": "Spain",      "foot": "Left"},
        {"name": "Florian Lejeune",    "pos": "DF", "age": 34, "nationality": "France",     "foot": "Right"},
        {"name": "Alejandro Catena",   "pos": "DF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Óscar Valentín",     "pos": "DF", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Isi Palazón",        "pos": "MF", "age": 31, "nationality": "Spain",      "foot": "Right"},
        {"name": "Jorge de Frutos",    "pos": "MF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Unai López",         "pos": "MF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Randy Nteka",        "pos": "MF", "age": 27, "nationality": "Congo",      "foot": "Right"},
        {"name": "Sergio Camello",     "pos": "FW", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Pathé Ciss",         "pos": "MF", "age": 32, "nationality": "Senegal",    "foot": "Right"},
        {"name": "Raúl de Tomás",      "pos": "FW", "age": 30, "nationality": "Spain",      "foot": "Right"},
    ],
    "Valencia": [
        {"name": "Giorgi Mamardashvili","pos": "GK", "age": 24, "nationality": "Georgia",   "foot": "Right"},
        {"name": "Thierry Correia",    "pos": "DF", "age": 27, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Hugo Guillamon",     "pos": "DF", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "César Tárrega",      "pos": "DF", "age": 21, "nationality": "Spain",      "foot": "Right"},
        {"name": "José Gayà",          "pos": "DF", "age": 30, "nationality": "Spain",      "foot": "Left"},
        {"name": "Pepelu",             "pos": "MF", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "Javi Guerra",        "pos": "MF", "age": 22, "nationality": "Spain",      "foot": "Right"},
        {"name": "André Almeida",      "pos": "MF", "age": 24, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Hugo Duro",          "pos": "FW", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "Rafa Mir",           "pos": "FW", "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Diego López",        "pos": "FW", "age": 22, "nationality": "Spain",      "foot": "Left"},
        {"name": "Justin Kluivert",    "pos": "FW", "age": 26, "nationality": "Netherlands","foot": "Right"},
    ],
    "Leganés": [
        {"name": "Marko Dmitrović",    "pos": "GK", "age": 33, "nationality": "Serbia",     "foot": "Right"},
        {"name": "Jonathan Silva",     "pos": "DF", "age": 31, "nationality": "Argentina",  "foot": "Left"},
        {"name": "Sergio González",    "pos": "DF", "age": 26, "nationality": "Spain",      "foot": "Right"},
        {"name": "Esteban Burgos",     "pos": "DF", "age": 27, "nationality": "Argentina",  "foot": "Right"},
        {"name": "Franchu Feuillassier","pos":"DF",  "age": 23, "nationality": "France",     "foot": "Left"},
        {"name": "Dani Raba",          "pos": "MF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Renato Tapia",       "pos": "MF", "age": 30, "nationality": "Peru",       "foot": "Right"},
        {"name": "Bryan Zaragoza",     "pos": "FW", "age": 23, "nationality": "Spain",      "foot": "Right"},
        {"name": "Miguel de la Fuente","pos": "FW",  "age": 28, "nationality": "Spain",      "foot": "Right"},
        {"name": "Yvan Neyou",         "pos": "MF", "age": 29, "nationality": "Cameroon",   "foot": "Right"},
        {"name": "Darko Brasanac",     "pos": "MF", "age": 33, "nationality": "Serbia",     "foot": "Right"},
        {"name": "Lautaro Morales",    "pos": "FW", "age": 25, "nationality": "Argentina",  "foot": "Right"},
    ],
    "Real Valladolid": [
        {"name": "Jordi Masip",        "pos": "GK", "age": 36, "nationality": "Spain",      "foot": "Right"},
        {"name": "Iván Fresneda",      "pos": "DF", "age": 21, "nationality": "Spain",      "foot": "Right"},
        {"name": "Joaquín Fernández",  "pos": "DF", "age": 32, "nationality": "Spain",      "foot": "Right"},
        {"name": "Marcos André",       "pos": "FW", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Cyle Larin",         "pos": "FW", "age": 29, "nationality": "Canada",     "foot": "Right"},
        {"name": "Selim Amallah",      "pos": "MF", "age": 29, "nationality": "Morocco",    "foot": "Right"},
        {"name": "Luis Pérez",         "pos": "MF", "age": 24, "nationality": "Spain",      "foot": "Right"},
        {"name": "Roque Mesa",         "pos": "MF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Rafa Janot",         "pos": "GK", "age": 44, "nationality": "Spain",      "foot": "Right"},
        {"name": "Óscar Plano",        "pos": "FW", "age": 33, "nationality": "Spain",      "foot": "Right"},
        {"name": "Kike Pérez",         "pos": "MF", "age": 30, "nationality": "Spain",      "foot": "Right"},
        {"name": "Idrissa Doumbia",    "pos": "MF", "age": 29, "nationality": "Ivory Coast","foot": "Right"},
    ],
    "Las Palmas": [
        {"name": "Álvaro Valles",      "pos": "GK", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Álex Suárez",        "pos": "DF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Sergi Cardona",      "pos": "DF", "age": 24, "nationality": "Spain",      "foot": "Left"},
        {"name": "Álex Muñoz",         "pos": "DF", "age": 27, "nationality": "Spain",      "foot": "Right"},
        {"name": "Mika Mármol",        "pos": "DF", "age": 25, "nationality": "Spain",      "foot": "Right"},
        {"name": "Óscar Clemente",     "pos": "MF", "age": 29, "nationality": "Spain",      "foot": "Right"},
        {"name": "Fabio Silva",        "pos": "FW", "age": 23, "nationality": "Portugal",   "foot": "Right"},
        {"name": "Alberto Moleiro",    "pos": "MF", "age": 21, "nationality": "Spain",      "foot": "Right"},
        {"name": "Sory Kaba",          "pos": "FW", "age": 30, "nationality": "Guinea",     "foot": "Right"},
        {"name": "Kirian Rodríguez",   "pos": "MF", "age": 31, "nationality": "Spain",      "foot": "Right"},
        {"name": "Jonathan Viera",     "pos": "MF", "age": 35, "nationality": "Spain",      "foot": "Right"},
        {"name": "Jesé Rodríguez",     "pos": "FW", "age": 32, "nationality": "Spain",      "foot": "Right"},
    ],
}

# Pad any team not in the dict with generated names
NATIONALITIES = ["Spain", "France", "Brazil", "Argentina", "Germany", "England",
                 "Portugal", "Italy", "Netherlands", "Belgium", "Uruguay", "Colombia"]

# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))

def rnorm(mean: float, std: float, lo: float = 0.0, hi: float = float("inf")) -> float:
    return clamp(random.gauss(mean, std), lo, hi)

def rtruncnorm_int(mean: float, std: float, lo: int = 0, hi: int = 999) -> int:
    return int(round(clamp(random.gauss(mean, std), lo, hi)))

def pos_simple(pos: str) -> str:
    if not pos or pos == "nan":
        return "MF"
    for p in ("GK", "DF", "MF", "FW"):
        if pos.startswith(p):
            return p
    return "MF"

# ─────────────────────────────────────────────────────────────────────────────
# STAT GENERATION (position-calibrated from real data)
# ─────────────────────────────────────────────────────────────────────────────

POS_PROFILES = {
    #               mp_mean mp_std  min_mean  min_std  goals  g_std  assists a_std  shots s_std  sot s_std2 yc yc_s rc rc_s  fc  fc_s  int i_s  tw tw_s  fouls_d
    "GK": dict(mp_m=19, mp_s=13, min_m=1300, min_s=900,
               g_m=0,   g_s=0,   a_m=0,   a_s=0,
               sh_m=0,  sh_s=0,  sot_m=0, sot_s=0,
               yc_m=1.1, yc_s=1.3, rc_m=0.07, rc_s=0.26,
               fc_m=0.3, fc_s=0.7, int_m=0, int_s=0, tw_m=0, tw_s=0, fd_m=0.3, fd_s=0.5),
    "DF": dict(mp_m=19, mp_s=11, min_m=1400, min_s=900,
               g_m=0.7,  g_s=1.1, a_m=1.0,  a_s=1.5,
               sh_m=10,  sh_s=10, sot_m=2.8, sot_s=3.4,
               yc_m=3.6, yc_s=3.0, rc_m=0.18, rc_s=0.43,
               fc_m=18,  fc_s=13, int_m=16, int_s=13, tw_m=18, tw_s=14, fd_m=12, fd_s=11),
    "MF": dict(mp_m=20, mp_s=12, min_m=1500, min_s=950,
               g_m=1.7,  g_s=2.4, a_m=1.5,  a_s=2.0,
               sh_m=19,  sh_s=17, sot_m=6.0, sot_s=6.6,
               yc_m=3.3, yc_s=3.0, rc_m=0.15, rc_s=0.39,
               fc_m=20,  fc_s=17, int_m=12, int_s=12, tw_m=16, tw_s=14, fd_m=17, fd_s=14),
    "FW": dict(mp_m=20, mp_s=12, min_m=1350, min_s=950,
               g_m=4.1,  g_s=4.9, a_m=1.9,  a_s=2.4,
               sh_m=31,  sh_s=27, sot_m=12,  sot_s=12,
               yc_m=2.2, yc_s=2.3, rc_m=0.12, rc_s=0.37,
               fc_m=17,  fc_s=14, int_m=5,  int_s=6, tw_m=9, tw_s=8, fd_m=22, fd_s=15),
}

GK_EXTRA_PROFILE = dict(
    sv_m=70,   sv_s=50,   # saves
    svp_m=0.72, svp_s=0.08, # save %
    cs_m=8,    cs_s=6,    # clean sheets
    gc_m=30,   gc_s=22,   # goals conceded
    w_m=9,     w_s=7,
    d_m=6,     d_s=4,
    l_m=8,     l_s=6,
)

# ─────────────────────────────────────────────────────────────────────────────
# SALARY LOGIC (weekly, EUR)
# ─────────────────────────────────────────────────────────────────────────────

TEAM_SALARY_TIER = {
    "Real Madrid": (120_000, 50_000), "Barcelona": (100_000, 45_000),
    "Atlético Madrid": (70_000, 30_000), "Athletic Club": (40_000, 15_000),
    "Real Sociedad": (35_000, 12_000), "Real Betis": (30_000, 12_000),
    "Villarreal": (35_000, 12_000), "Sevilla": (30_000, 12_000),
    "Celta Vigo": (20_000, 8_000), "Girona": (25_000, 10_000),
    "Osasuna": (15_000, 5_000), "Mallorca": (15_000, 5_000),
    "Getafe": (15_000, 5_000), "Rayo Vallecano": (12_000, 4_000),
    "Espanyol": (18_000, 7_000), "Alavés": (12_000, 4_000),
    "Leganés": (12_000, 4_000), "Valencia": (20_000, 8_000),
    "Real Valladolid": (10_000, 4_000), "Las Palmas": (10_000, 4_000),
}

def generate_salary(team: str, pos: str) -> tuple[int, int]:
    base_m, base_s = TEAM_SALARY_TIER.get(team, (15_000, 5_000))
    pos_mult = {"GK": 0.9, "DF": 0.85, "MF": 1.0, "FW": 1.15}.get(pos_simple(pos), 1.0)
    weekly = max(3_000, int(round(rnorm(base_m * pos_mult, base_s), -3)))
    annual = weekly * 52
    return weekly, annual

# ─────────────────────────────────────────────────────────────────────────────
# PLAYER STAT GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def generate_player_stats(player: dict, team: str, season: str = SEASON) -> dict:
    pname   = player["name"]
    pos     = player.get("pos", "MF")
    age     = player.get("age", 26)
    nat     = player.get("nationality", "Spain")
    foot    = player.get("foot", "Right")
    height  = player.get("height", rtruncnorm_int(180, 8, 160, 205))
    weight  = player.get("weight", rtruncnorm_int(75, 8, 60, 100))

    p = POS_PROFILES[pos_simple(pos)]
    mp  = rtruncnorm_int(p["mp_m"],  p["mp_s"],  0, 38)
    if mp == 0:
        # bench warmer — minimal stats
        return _empty_row(pname, pos, age, nat, foot, height, weight, team, season)

    # Minutes scale factor
    min_played = rtruncnorm_int(p["min_m"], p["min_s"], 0, mp * 95)
    starts     = rtruncnorm_int(mp * 0.7, mp * 0.2, 0, mp)
    full_90s   = round(min_played / 90, 1)

    goals   = rtruncnorm_int(p["g_m"],   p["g_s"],   0, 41)
    assists = rtruncnorm_int(p["a_m"],   p["a_s"],   0, 17)
    shots   = rtruncnorm_int(p["sh_m"],  p["sh_s"],  goals, 165)
    sot     = rtruncnorm_int(min(p["sot_m"], shots), p["sot_s"], goals, shots + 1)
    yc      = rtruncnorm_int(p["yc_m"],  p["yc_s"],  0, 18)
    rc      = rtruncnorm_int(p["rc_m"],  p["rc_s"],  0, 3)
    fc      = rtruncnorm_int(p["fc_m"],  p["fc_s"],  0, 85)
    fd      = rtruncnorm_int(p["fd_m"],  p["fd_s"],  0, 85)
    int_    = rtruncnorm_int(p["int_m"], p["int_s"], 0, 70)
    tw      = rtruncnorm_int(p["tw_m"],  p["tw_s"],  0, 80)
    pen_att = rtruncnorm_int(0.3, 0.7, 0, 10) if pos_simple(pos) in ("FW", "MF") else 0
    pen_g   = rtruncnorm_int(pen_att * 0.7, pen_att * 0.3, 0, pen_att)
    npg     = max(0, goals - pen_g)
    gc_diff = rtruncnorm_int(0, 8, -20, 20)

    weekly_sal, annual_sal = generate_salary(team, pos)

    base_contract = random.choice([2027, 2028, 2029, 2030])

    row = {
        "player_name":                          pname,
        "player_position":                      pos,
        "preferred_foot":                       foot,
        "height_cm":                            height,
        "weight_kg":                            weight,
        "national_team":                        nat,
        "current_club":                         team,
        "contract_expiry":                      base_contract,
        "season":                               season,
        "player_age":                           age,
        "club":                                 team,
        "club_country":                         "ESP",
        "competition":                          COMPETITION,
        "league_rank":                          None,           # filled after table is built
        "matches_played":                       mp,
        "matches_started":                      starts,
        "minutes_played":                       min_played,
        "full_90s_played":                      full_90s,
        "goals":                                goals,
        "assists":                              assists,
        "goal_contributions":                   goals + assists,
        "non_penalty_goals":                    npg,
        "penalty_goals":                        pen_g,
        "penalty_attempts":                     pen_att,
        "yellow_cards":                         yc,
        "red_cards":                            rc,
        "goals_per_90":                         round(goals / full_90s, 2) if full_90s else 0.0,
        "assists_per_90":                       round(assists / full_90s, 2) if full_90s else 0.0,
        "goal_contributions_per_90":            round((goals+assists) / full_90s, 2) if full_90s else 0.0,
        "non_penalty_goals_per_90":             round(npg / full_90s, 2) if full_90s else 0.0,
        "non_penalty_goal_contributions_per_90":round((npg+assists) / full_90s, 2) if full_90s else 0.0,
        "matches_link":                         f"https://fbref.com/en/players/fake/{pname.lower().replace(' ', '-')}",
        "total_shots":                          shots,
        "shots_on_target":                      sot,
        "shots_on_target_percentage":           round(sot / shots * 100, 1) if shots else 0.0,
        "shots_per_90":                         round(shots / full_90s, 2) if full_90s else 0.0,
        "shots_on_target_per_90":               round(sot / full_90s, 2) if full_90s else 0.0,
        "goals_per_shot":                       round(goals / shots, 2) if shots else 0.0,
        "goals_per_shot_on_target":             round(goals / sot, 2) if sot else 0.0,
        "minutes_per_match":                    round(min_played / mp, 1) if mp else 0.0,
        "minutes_percentage":                   round(min_played / (38 * 90) * 100, 1),
        "minutes_per_start":                    round(min_played / starts, 1) if starts else 0.0,
        "full_matches_completed":               rtruncnorm_int(starts * 0.6, starts * 0.2, 0, starts),
        "minutes_per_substitution":             round(random.uniform(45, 80), 1) if mp > starts else None,
        "unused_substitute_matches":            rtruncnorm_int(3, 3, 0, 15),
        "points_per_match":                     round(random.uniform(1.0, 2.2), 2),
        "team_goals_while_on_pitch":            rtruncnorm_int(30 * min_played / 3420, 5, 0, 80),
        "team_goals_conceded_while_on_pitch":   rtruncnorm_int(28 * min_played / 3420, 5, 0, 80),
        "goal_difference_on_pitch":             gc_diff,
        "goal_difference_per_90":               round(gc_diff / full_90s, 2) if full_90s else 0.0,
        "on_off_difference":                    rtruncnorm_int(0, 5, -15, 15),
        "fouls_committed":                      fc,
        "fouls_drawn":                          fd,
        "offsides":                             rtruncnorm_int(2, 3, 0, 25) if pos_simple(pos) == "FW" else 0,
        "crosses":                              rtruncnorm_int(10, 15, 0, 80) if pos_simple(pos) in ("DF","MF","FW") else 0,
        "interceptions":                        int_,
        "tackles_won":                          tw,
        "penalties_won":                        rtruncnorm_int(0.4, 0.7, 0, 6),
        "penalties_conceded":                   rtruncnorm_int(0.2, 0.5, 0, 4),
        "own_goals":                            rtruncnorm_int(0.05, 0.22, 0, 2),
        "weekly_salary":                        weekly_sal,
        "annual_salary":                        annual_sal,
        # GK-specific (NaN for outfield)
        "goals_conceded":                       None,
        "goals_conceded_per_90":                None,
        "shots_on_target_against":              None,
        "saves":                                None,
        "save_percentage":                      None,
        "wins":                                 None,
        "draws":                                None,
        "losses":                               None,
        "clean_sheets":                         None,
        "clean_sheet_percentage":               None,
        "penalties_faced":                      None,
        "penalty_goals_conceded":               None,
    }

    # GK overrides
    if pos_simple(pos) == "GK":
        gk = GK_EXTRA_PROFILE
        gc  = rtruncnorm_int(gk["gc_m"], gk["gc_s"], 0, 80)
        sv  = rtruncnorm_int(gk["sv_m"], gk["sv_s"], gc, 200)
        svp = round(clamp(random.gauss(gk["svp_m"], gk["svp_s"]), 0.50, 0.95), 3)
        w   = rtruncnorm_int(gk["w_m"],  gk["w_s"],  0, 38)
        d   = rtruncnorm_int(gk["d_m"],  gk["d_s"],  0, 38 - w)
        l   = max(0, mp - w - d)
        cs  = rtruncnorm_int(gk["cs_m"], gk["cs_s"], 0, w)
        pf  = rtruncnorm_int(2, 2, 0, 10)
        pgc = rtruncnorm_int(pf * 0.25, 0.5, 0, pf)
        row.update({
            "goals_conceded":          gc,
            "goals_conceded_per_90":   round(gc / full_90s, 2) if full_90s else 0,
            "shots_on_target_against": sv + gc,
            "saves":                   sv,
            "save_percentage":         svp,
            "wins":                    w,
            "draws":                   d,
            "losses":                  l,
            "clean_sheets":            cs,
            "clean_sheet_percentage":  round(cs / mp * 100, 1) if mp else 0,
            "penalties_faced":         pf,
            "penalty_goals_conceded":  pgc,
            # reset irrelevant outfield stats for GK
            "goals": 0, "assists": 0, "goal_contributions": 0,
            "non_penalty_goals": 0, "penalty_goals": 0, "penalty_attempts": 0,
            "total_shots": 0, "shots_on_target": 0,
        })

    return row


def _empty_row(pname, pos, age, nat, foot, height, weight, team, season):
    d = {k: 0 for k in [
        "matches_played","matches_started","minutes_played","full_90s_played",
        "goals","assists","goal_contributions","non_penalty_goals","penalty_goals",
        "penalty_attempts","yellow_cards","red_cards","total_shots","shots_on_target",
        "fouls_committed","fouls_drawn","offsides","crosses","interceptions",
        "tackles_won","penalties_won","penalties_conceded","own_goals","unused_substitute_matches"
    ]}
    d.update({
        "player_name": pname, "player_position": pos, "preferred_foot": foot,
        "height_cm": height, "weight_kg": weight, "national_team": nat,
        "current_club": team, "contract_expiry": 2028, "season": season,
        "player_age": age, "club": team, "club_country": "Spain",
        "competition": COMPETITION, "league_rank": None,
        "goals_per_90": 0, "assists_per_90": 0, "goal_contributions_per_90": 0,
        "non_penalty_goals_per_90": 0, "non_penalty_goal_contributions_per_90": 0,
        "matches_link": "", "shots_on_target_percentage": 0, "shots_per_90": 0,
        "shots_on_target_per_90": 0, "goals_per_shot": 0, "goals_per_shot_on_target": 0,
        "minutes_per_match": 0, "minutes_percentage": 0, "minutes_per_start": 0,
        "full_matches_completed": 0, "minutes_per_substitution": None,
        "points_per_match": 0, "team_goals_while_on_pitch": 0,
        "team_goals_conceded_while_on_pitch": 0, "goal_difference_on_pitch": 0,
        "goal_difference_per_90": 0, "on_off_difference": 0,
        "weekly_salary": generate_salary(team, pos)[0],
        "annual_salary": generate_salary(team, pos)[1],
        **{k: None for k in ["goals_conceded","goals_conceded_per_90","shots_on_target_against",
            "saves","save_percentage","wins","draws","losses","clean_sheets",
            "clean_sheet_percentage","penalties_faced","penalty_goals_conceded"]},
    })
    return d


# ─────────────────────────────────────────────────────────────────────────────
# PLAYER STATS OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def generate_all_player_stats(output_dir: str):
    """Write one CSV per player, organised in team subdirectories."""
    base = os.path.join(output_dir, "La Liga")
    os.makedirs(base, exist_ok=True)

    total = 0
    for team in LA_LIGA_TEAMS:
        team_dir = os.path.join(base, team)
        os.makedirs(team_dir, exist_ok=True)
        squad = SQUADS.get(team, [])

        # Pad squad to ~22 players if needed
        while len(squad) < 22:
            squad.append({
                "name": f"Player {team[:3]} {len(squad)+1}",
                "pos": random.choice(["DF","DF","MF","MF","FW","FW","GK"]),
                "age": random.randint(19, 33),
                "nationality": random.choice(NATIONALITIES),
                "foot": random.choice(["Right","Right","Right","Left"]),
            })

        for player in squad:
            stats = generate_player_stats(player, team)
            df = pd.DataFrame([stats])
            safe_name = player["name"].replace("/", "-").replace("\\", "-")
            fpath = os.path.join(team_dir, f"{safe_name}.csv")
            df.to_csv(fpath, index=False)
            total += 1

        print(f"  ✓  {team:25s}  {len(squad)} players")

    print(f"\nTotal player files written: {total}")


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFER MARKET VALUES
# ─────────────────────────────────────────────────────────────────────────────

MV_SNAPSHOTS_PER_SEASON = [
    ("2026-07-01", "pre-season"),
    ("2026-08-15", "transfer window closes"),
    ("2026-10-01", "autumn"),
    ("2027-01-10", "winter window opens"),
    ("2027-02-01", "winter window closes"),
    ("2027-04-01", "spring"),
    ("2027-06-01", "end of season"),
]

def base_market_value(team: str, pos: str) -> float:
    """Random starting market value in EUR."""
    budget   = TEAM_BUDGET.get(team, 120) * 1_000_000
    n_squad  = len(SQUADS.get(team, [{"x": 1}] * 22))
    avg      = budget / n_squad
    pos_mult = {"GK": 0.7, "DF": 0.8, "MF": 1.0, "FW": 1.3}.get(pos, 1.0)
    val = rnorm(avg * pos_mult, avg * pos_mult * 0.5, 500_000, budget * 0.6)
    # Round to nearest 500k
    return round(val / 500_000) * 500_000

def evolve_value(prev: float, event: str) -> float:
    """Apply realistic drift to market value between snapshots."""
    drift_map = {
        "pre-season":              (1.00, 0.05),
        "transfer window closes":  (1.03, 0.08),
        "autumn":                  (1.02, 0.06),
        "winter window opens":     (0.99, 0.07),
        "winter window closes":    (1.01, 0.06),
        "spring":                  (1.02, 0.05),
        "end of season":           (1.00, 0.06),
    }
    mu, sigma = drift_map.get(event, (1.0, 0.05))
    factor = random.gauss(mu, sigma)
    new_val = prev * factor
    return max(100_000, round(new_val / 250_000) * 250_000)

def generate_transfer_values(output_dir: str):
    rows = []
    pid_counter = 900_000

    for team in LA_LIGA_TEAMS:
        squad = SQUADS.get(team, [])
        for player in squad:
            pid  = pid_counter; pid_counter += 1
            pname= player["name"]
            pos  = player.get("pos", "MF")
            age  = player.get("age", 26)

            current_mv = base_market_value(team, pos_simple(pos))

            for snap_date, event in MV_SNAPSHOTS_PER_SEASON:
                # Age players by 1 at end of season
                snap_age = age if snap_date < "2027-01-01" else age
                row = {
                    "player_name":       pname,
                    "player_id":         pid,
                    "league":            "LaLiga",
                    "club":              team,
                    "date":              snap_date,
                    "age":               snap_age,
                    "market_value_eur":  current_mv,
                    "season":            SEASON,
                }
                rows.append(row)
                current_mv = evolve_value(current_mv, event)

    df = pd.DataFrame(rows)
    os.makedirs(output_dir, exist_ok=True)
    fpath = os.path.join(output_dir, "transfer_market_values_2026_27.csv")
    df.to_csv(fpath, index=False)
    print(f"Transfer market values written: {len(df)} rows → {fpath}")


# ─────────────────────────────────────────────────────────────────────────────
# MATCH RESULTS
# ─────────────────────────────────────────────────────────────────────────────

# La Liga 2026-27 starts late August 2026
MATCHDAY_START = date(2026, 8, 22)
DAYS_BETWEEN_MATCHDAYS = 7

def team_strength(team: str) -> float:
    """0–1 score used to bias goals and results."""
    order = [
        "Real Madrid", "Barcelona", "Atlético Madrid", "Athletic Club",
        "Real Sociedad", "Villarreal", "Real Betis", "Sevilla", "Girona",
        "Celta Vigo", "Valencia", "Osasuna", "Mallorca", "Espanyol",
        "Rayo Vallecano", "Leganés", "Getafe", "Alavés", "Las Palmas",
        "Real Valladolid",
    ]
    idx = order.index(team) if team in order else 10
    return 1.0 - idx / len(order)

def simulate_match(home: str, away: str) -> dict:
    hs = team_strength(home) + 0.08        # home advantage
    as_ = team_strength(away)

    # Expected goals (Poisson-ish)
    hxg = max(0.3, rnorm(1.4 * hs / (hs + as_) * 2.8, 0.6))
    axg = max(0.2, rnorm(1.4 * as_ / (hs + as_) * 2.8, 0.6))

    hg = int(np.random.poisson(hxg))
    ag = int(np.random.poisson(axg))

    result = "H" if hg > ag else ("A" if ag > hg else "D")

    return {
        "home_team": home, "away_team": away,
        "home_goals": hg,  "away_goals": ag,
        "home_xg":   round(hxg, 2), "away_xg": round(axg, 2),
        "result": result,
        "home_shots":       rtruncnorm_int(12, 4, 3, 28),
        "away_shots":       rtruncnorm_int(10, 4, 2, 25),
        "home_possession":  round(rnorm(50 + (hs - as_) * 20, 8, 25, 75), 1),
        "home_yellow_cards": rtruncnorm_int(2.0, 1.2, 0, 6),
        "away_yellow_cards": rtruncnorm_int(2.2, 1.2, 0, 6),
        "home_red_cards":    rtruncnorm_int(0.08, 0.28, 0, 2),
        "away_red_cards":    rtruncnorm_int(0.08, 0.28, 0, 2),
        "home_corners":     rtruncnorm_int(5, 2, 0, 14),
        "away_corners":     rtruncnorm_int(4, 2, 0, 12),
    }

def build_schedule(teams: list[str]) -> list[dict]:
    """Round-robin double-header schedule (38 matchdays)."""
    n = len(teams)
    fixed = teams[0]
    rotating = teams[1:]
    fixtures = []

    for rnd in range(n - 1):
        pairs = [(fixed, rotating[rnd])]
        for i in range(1, n // 2):
            h = rotating[(rnd + i) % (n - 1)]
            a = rotating[(rnd - i) % (n - 1)]
            pairs.append((h, a))
        fixtures.append(pairs)

    # Second leg — swap home/away
    second_leg = [[(a, h) for h, a in rnd_pairs] for rnd_pairs in fixtures]
    return fixtures + second_leg

def generate_match_results(output_dir: str, stream: bool = False, matchday: Optional[int] = None):
    schedule = build_schedule(LA_LIGA_TEAMS)
    rows = []
    standings: dict[str, dict] = {
        t: {"team": t, "P": 0, "W": 0, "D": 0, "L": 0, "GF": 0, "GA": 0, "GD": 0, "Pts": 0}
        for t in LA_LIGA_TEAMS
    }

    for md_idx, round_pairs in enumerate(schedule, start=1):
        md_date = MATCHDAY_START + timedelta(days=(md_idx - 1) * DAYS_BETWEEN_MATCHDAYS)
        for home, away in round_pairs:
            match = simulate_match(home, away)
            match["matchday"] = md_idx
            match["date"]     = str(md_date)
            match["season"]   = SEASON
            rows.append(match)

            hg, ag = match["home_goals"], match["away_goals"]
            for t, gf, ga, is_home in [(home, hg, ag, True), (away, ag, hg, False)]:
                standings[t]["P"]  += 1
                standings[t]["GF"] += gf
                standings[t]["GA"] += ga
                standings[t]["GD"] += gf - ga
                if gf > ga:
                    standings[t]["W"] += 1; standings[t]["Pts"] += 3
                elif gf == ga:
                    standings[t]["D"] += 1; standings[t]["Pts"] += 1
                else:
                    standings[t]["L"] += 1

            if stream and matchday and md_idx == matchday:
                print(json.dumps(match, indent=2))
                time.sleep(0.05)   # simulate streaming delay

    os.makedirs(output_dir, exist_ok=True)

    # Matches CSV
    df_matches = pd.DataFrame(rows)
    mpath = os.path.join(output_dir, "match_results_2026_27.csv")
    df_matches.to_csv(mpath, index=False)
    print(f"Match results written: {len(df_matches)} rows → {mpath}")

    # Standings CSV
    df_table = (
        pd.DataFrame(standings.values())
          .sort_values(["Pts", "GD", "GF"], ascending=False)
          .reset_index(drop=True)
    )
    df_table.insert(0, "Pos", range(1, len(df_table) + 1))
    tpath = os.path.join(output_dir, "standings_2026_27.csv")
    df_table.to_csv(tpath, index=False)
    print(f"Final standings written → {tpath}")

    return df_table


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic La Liga 2026-2027 data for pipeline testing."
    )
    parser.add_argument(
        "--mode", choices=["player_stats", "transfer_values", "match_results", "all"],
        default="all", help="Which dataset(s) to generate."
    )
    parser.add_argument(
        "--output", default="./generated_data_2026_27",
        help="Root output directory."
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility."
    )
    parser.add_argument(
        "--stream", action="store_true",
        help="Stream match results to stdout (simulates API/scraper feed)."
    )
    parser.add_argument(
        "--matchday", type=int, default=None,
        help="When --stream is set, stream only this matchday number."
    )
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    print(f"\n{'='*55}")
    print(f"  La Liga {SEASON} Data Generator")
    print(f"  Mode   : {args.mode}")
    print(f"  Output : {args.output}")
    print(f"  Seed   : {args.seed}")
    print(f"{'='*55}\n")

    if args.mode in ("player_stats", "all"):
        print("── Generating player stats …")
        generate_all_player_stats(args.output)

    if args.mode in ("transfer_values", "all"):
        print("\n── Generating transfer market values …")
        generate_transfer_values(args.output)

    if args.mode in ("match_results", "all"):
        print("\n── Generating match results & standings …")
        table = generate_match_results(args.output, stream=args.stream, matchday=args.matchday)
        print("\n  Final Standings Preview:")
        print(table[["Pos","team","P","W","D","L","GF","GA","GD","Pts"]].to_string(index=False))

    print(f"\n✅  Done. All files saved to: {args.output}\n")


if __name__ == "__main__":
    main()