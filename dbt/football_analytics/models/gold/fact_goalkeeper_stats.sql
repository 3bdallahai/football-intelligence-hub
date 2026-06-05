{{ config(
    materialized='table'
) }}

WITH gk AS (

    SELECT *
    FROM {{ ref('stg_players') }}
    WHERE UPPER(TRIM(player_position)) = 'GK'

),

dim_season AS (

    SELECT *
    FROM {{ ref('dim_season') }}

),

dim_player AS (

    SELECT *
    FROM {{ ref('dim_player') }}

),

dim_club AS (

    SELECT *
    FROM {{ ref('dim_club') }}

)

SELECT

    /* ── KEYS ───────────────────────────────────────────────────────────── */
    p.player_key,
    c.club_key,
    ds.season_key,

    /* ── DEGENERATE DIMENSIONS (SAFE FOR DEBUGGING ONLY) ────────────────── */
    gk.player_name,
    ds.season AS season,
    gk.club,
    gk.competition,
    gk.player_age,

    /* ── PLAYING TIME ───────────────────────────────────────────────────── */
    gk.matches_played,
    gk.matches_started,
    gk.minutes_played,
    gk.full_90s_played,
    gk.minutes_per_match,
    gk.minutes_percentage,
    gk.minutes_per_start,
    gk.full_matches_completed,
    gk.points_per_match,

    /* ── GK CORE: CONCEDING & SAVING ────────────────────────────────────── */
    gk.goals_conceded,
    gk.goals_conceded_per_90,
    gk.shots_on_target_against,
    gk.saves,
    gk.save_percentage,

    /* ── GK RESULTS ─────────────────────────────────────────────────────── */
    gk.wins,
    gk.draws,
    gk.losses,
    gk.clean_sheets,
    gk.clean_sheet_percentage,

    /* ── PENALTIES ──────────────────────────────────────────────────────── */
    gk.penalties_faced,
    gk.penalty_goals_conceded,
    gk.penalties_conceded,

    /* ── TEAM IMPACT ────────────────────────────────────────────────────── */
    gk.team_goals_conceded_while_on_pitch,
    gk.goal_difference_on_pitch,
    gk.goal_difference_per_90,
    gk.on_off_difference,

    /* ── DISCIPLINE ─────────────────────────────────────────────────────── */
    gk.yellow_cards,
    gk.red_cards,
    gk.fouls_committed,
    gk.own_goals,

    /* ── SALARY ─────────────────────────────────────────────────────────── */
    gk.weekly_salary_gbp,
    gk.weekly_salary_eur,
    gk.weekly_salary_usd,
    gk.annual_salary_gbp,
    gk.annual_salary_eur,
    gk.annual_salary_usd

FROM gk

LEFT JOIN dim_season ds
    ON gk.season = ds.season

LEFT JOIN dim_player p
    ON p.player_name = gk.player_name

LEFT JOIN dim_club c
    ON c.club_name = gk.club