{{ config(
    materialized='table'
) }}

WITH source AS (

    SELECT * FROM {{ source('bronze', 'BRONZE_PLAYERS') }}

),

cleaned AS (

    SELECT

        -- ── IDENTIFIERS ─────────────────────────────────────────────
        player_name,
        player_position,
        preferred_foot,
        national_team,
        current_club,
        _source_file,

        -- ── PHYSICAL ───────────────────────────────────────────────
        TRY_CAST(height_cm AS INTEGER) AS height_cm,
        TRY_CAST(weight_kg AS INTEGER) AS weight_kg,

        -- ── CONTRACT ───────────────────────────────────────────────
        TRY_TO_DATE(contract_expiry, 'MMMM YYYY') AS contract_expiry,

        -- ── CONTEXT ────────────────────────────────────────────────
        season,
        TRY_CAST(player_age AS INTEGER) AS player_age,
        club,
        UPPER(RIGHT(TRIM(club_country), 3)) AS club_country,
        TRIM(REGEXP_REPLACE(competition, '^[0-9]+\\.\\s*', '')) AS competition,
        TRY_CAST(REGEXP_REPLACE(league_rank, '[^0-9]', '') AS INTEGER) AS league_rank,

        -- ── SALARY ────────────────────────────────────────────────
        TRY_CAST(REPLACE(REGEXP_SUBSTR(weekly_salary, '£[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS weekly_salary_gbp,
        TRY_CAST(REPLACE(REGEXP_SUBSTR(weekly_salary, '€[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS weekly_salary_eur,
        TRY_CAST(REPLACE(REGEXP_SUBSTR(weekly_salary, '\\$[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS weekly_salary_usd,

        TRY_CAST(REPLACE(REGEXP_SUBSTR(annual_salary, '£[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS annual_salary_gbp,
        TRY_CAST(REPLACE(REGEXP_SUBSTR(annual_salary, '€[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS annual_salary_eur,
        TRY_CAST(REPLACE(REGEXP_SUBSTR(annual_salary, '\\$[\\s]?([0-9,]+)', 1, 1, 'e', 1), ',', '') AS INTEGER) AS annual_salary_usd,

        -- ── MATCHES ───────────────────────────────────────────────
        TRY_CAST(matches_played AS INTEGER) AS matches_played,
        TRY_CAST(matches_started AS INTEGER) AS matches_started,
        TRY_CAST(minutes_played AS INTEGER) AS minutes_played,
        TRY_CAST(full_90s_played AS FLOAT) AS full_90s_played,

        -- ── GOALS / ATTACK ─────────────────────────────────────────
        TRY_CAST(goals AS INTEGER) AS goals,
        TRY_CAST(assists AS INTEGER) AS assists,
        TRY_CAST(goal_contributions AS INTEGER) AS goal_contributions,
        TRY_CAST(non_penalty_goals AS INTEGER) AS non_penalty_goals,
        TRY_CAST(penalty_goals AS INTEGER) AS penalty_goals,
        TRY_CAST(penalty_attempts AS INTEGER) AS penalty_attempts,


        TRY_CAST(goals_per_90 AS FLOAT) AS goals_per_90,
        TRY_CAST(assists_per_90 AS FLOAT) AS assists_per_90,
        TRY_CAST(goal_contributions_per_90 AS FLOAT) AS goal_contributions_per_90,
        TRY_CAST(non_penalty_goals_per_90 AS FLOAT) AS non_penalty_goals_per_90,
        TRY_CAST(non_penalty_goal_contributions_per_90 AS FLOAT) AS non_penalty_goal_contributions_per_90,

        -- ── SHOOTING ───────────────────────────────────────────────
        TRY_CAST(total_shots AS FLOAT) AS total_shots,
        TRY_CAST(shots_on_target AS FLOAT) AS shots_on_target,
        TRY_CAST(shots_on_target_percentage AS FLOAT) AS shots_on_target_percentage,
        TRY_CAST(shots_per_90 AS FLOAT) AS shots_per_90,
        TRY_CAST(shots_on_target_per_90 AS FLOAT) AS shots_on_target_per_90,
        TRY_CAST(goals_per_shot AS FLOAT) AS goals_per_shot,
        TRY_CAST(goals_per_shot_on_target AS FLOAT) AS goals_per_shot_on_target,

        -- ── MINUTES ───────────────────────────────────────────────
        TRY_CAST(minutes_per_match AS FLOAT) AS minutes_per_match,
        TRY_CAST(minutes_percentage AS FLOAT) AS minutes_percentage,
        TRY_CAST(minutes_per_start AS FLOAT) AS minutes_per_start,
        TRY_CAST(full_matches_completed AS INTEGER) AS full_matches_completed,
        TRY_CAST(minutes_per_substitution AS FLOAT) AS minutes_per_substitution,
        TRY_CAST(unused_substitute_matches AS INTEGER) AS unused_substitute_matches,
        TRY_CAST(points_per_match AS FLOAT) AS points_per_match,

        -- ── TEAM IMPACT ───────────────────────────────────────────
        TRY_CAST(team_goals_while_on_pitch AS INTEGER) AS team_goals_while_on_pitch,
        TRY_CAST(team_goals_conceded_while_on_pitch AS INTEGER) AS team_goals_conceded_while_on_pitch,
        TRY_CAST(goal_difference_on_pitch AS INTEGER) AS goal_difference_on_pitch,
        TRY_CAST(goal_difference_per_90 AS FLOAT) AS goal_difference_per_90,
        TRY_CAST(on_off_difference AS FLOAT) AS on_off_difference,

        -- ── DISCIPLINE / MISC ─────────────────────────────────────
        TRY_CAST(yellow_cards AS INTEGER) AS yellow_cards,
        TRY_CAST(red_cards AS INTEGER) AS red_cards,
        TRY_CAST(fouls_committed AS FLOAT) AS fouls_committed,
        TRY_CAST(fouls_drawn AS FLOAT) AS fouls_drawn,
        TRY_CAST(offsides AS FLOAT) AS offsides,
        TRY_CAST(crosses AS FLOAT) AS crosses,
        TRY_CAST(interceptions AS FLOAT) AS interceptions,
        TRY_CAST(tackles_won AS FLOAT) AS tackles_won,
        TRY_CAST(penalties_won AS FLOAT) AS penalties_won,
        TRY_CAST(penalties_conceded AS FLOAT) AS penalties_conceded,
        TRY_CAST(own_goals AS INTEGER) AS own_goals,

        -- ── GK STATS ──────────────────────────────────────────────
        TRY_CAST(goals_conceded AS INTEGER) AS goals_conceded,
        TRY_CAST(goals_conceded_per_90 AS FLOAT) AS goals_conceded_per_90,
        TRY_CAST(shots_on_target_against AS INTEGER) AS shots_on_target_against,
        TRY_CAST(saves AS INTEGER) AS saves,
        TRY_CAST(save_percentage AS FLOAT) AS save_percentage,
        TRY_CAST(wins AS INTEGER) AS wins,
        TRY_CAST(draws AS INTEGER) AS draws,
        TRY_CAST(losses AS INTEGER) AS losses,
        TRY_CAST(clean_sheets AS INTEGER) AS clean_sheets,
        TRY_CAST(clean_sheet_percentage AS FLOAT) AS clean_sheet_percentage,
        TRY_CAST(penalties_faced AS INTEGER) AS penalties_faced,
        TRY_CAST(penalty_goals_conceded AS INTEGER) AS penalty_goals_conceded

    FROM source
    WHERE player_name IS NOT NULL
      AND season IS NOT NULL

),

deduplicated AS (

    SELECT

        player_name,
        season,
        club,
        competition,

        /* ── IDENTITY ── */
        MAX(player_position) AS player_position,
        MAX(preferred_foot) AS preferred_foot,
        MAX(national_team) AS national_team,
        MAX(current_club) AS current_club,
        MAX(_source_file) AS _source_file,

        /* ── PHYSICAL ── */
        MAX(height_cm) AS height_cm,
        MAX(weight_kg) AS weight_kg,
        MAX(player_age) AS player_age,
        MAX(club_country) AS club_country,
        MAX(league_rank) AS league_rank,
        MAX(contract_expiry) AS contract_expiry,

        /* ── SALARY ── */
        MAX(weekly_salary_gbp) AS weekly_salary_gbp,
        MAX(weekly_salary_eur) AS weekly_salary_eur,
        MAX(weekly_salary_usd) AS weekly_salary_usd,
        MAX(annual_salary_gbp) AS annual_salary_gbp,
        MAX(annual_salary_eur) AS annual_salary_eur,
        MAX(annual_salary_usd) AS annual_salary_usd,

        /* ── MATCHES ── */
        MAX(matches_played) AS matches_played,
        MAX(matches_started) AS matches_started,
        MAX(minutes_played) AS minutes_played,
        MAX(full_90s_played) AS full_90s_played,

        /* ── ATTACK ── */
        MAX(goals) AS goals,
        MAX(assists) AS assists,
        MAX(goal_contributions) AS goal_contributions,
        MAX(non_penalty_goals) AS non_penalty_goals,
        MAX(penalty_goals) AS penalty_goals,
        MAX(penalty_attempts) AS penalty_attempts,

        max(goals_per_90) AS goals_per_90,
        max(assists_per_90) AS assists_per_90,
        max(goal_contributions_per_90) AS goal_contributions_per_90,
        max(non_penalty_goals_per_90) AS non_penalty_goals_per_90,
        max(non_penalty_goal_contributions_per_90) AS non_penalty_goal_contributions_per_90,

        /* ── SHOOTING ── */
        MAX(total_shots) AS total_shots,
        MAX(shots_on_target) AS shots_on_target,
        MAX(shots_on_target_percentage) AS shots_on_target_percentage,
        MAX(shots_per_90) AS shots_per_90,
        MAX(shots_on_target_per_90) AS shots_on_target_per_90,
        MAX(goals_per_shot) AS goals_per_shot,
        MAX(goals_per_shot_on_target) AS goals_per_shot_on_target,

        /* ── MINUTES ── */
        MAX(minutes_per_match) AS minutes_per_match,
        MAX(minutes_percentage) AS minutes_percentage,
        MAX(minutes_per_start) AS minutes_per_start,
        MAX(full_matches_completed) AS full_matches_completed,
        MAX(minutes_per_substitution) AS minutes_per_substitution,
        MAX(unused_substitute_matches) AS unused_substitute_matches,
        MAX(points_per_match) AS points_per_match,

        /* ── TEAM ── */
        MAX(team_goals_while_on_pitch) AS team_goals_while_on_pitch,
        MAX(team_goals_conceded_while_on_pitch) AS team_goals_conceded_while_on_pitch,
        MAX(goal_difference_on_pitch) AS goal_difference_on_pitch,
        MAX(goal_difference_per_90) AS goal_difference_per_90,
        MAX(on_off_difference) AS on_off_difference,

        /* ── DISCIPLINE ── */
        MAX(yellow_cards) AS yellow_cards,
        MAX(red_cards) AS red_cards,
        MAX(fouls_committed) AS fouls_committed,
        MAX(fouls_drawn) AS fouls_drawn,
        MAX(offsides) AS offsides,
        MAX(crosses) AS crosses,
        MAX(interceptions) AS interceptions,
        MAX(tackles_won) AS tackles_won,
        MAX(penalties_won) AS penalties_won,
        MAX(penalties_conceded) AS penalties_conceded,
        MAX(own_goals) AS own_goals,

        /* ── GK ── */
        MAX(goals_conceded) AS goals_conceded,
        MAX(goals_conceded_per_90) AS goals_conceded_per_90,
        MAX(shots_on_target_against) AS shots_on_target_against,
        MAX(saves) AS saves,
        MAX(save_percentage) AS save_percentage,
        MAX(wins) AS wins,
        MAX(draws) AS draws,
        MAX(losses) AS losses,
        MAX(clean_sheets) AS clean_sheets,
        MAX(clean_sheet_percentage) AS clean_sheet_percentage,
        MAX(penalties_faced) AS penalties_faced,
        MAX(penalty_goals_conceded) AS penalty_goals_conceded

    FROM cleaned
    GROUP BY player_name, season, club, competition

)

SELECT * FROM deduplicated