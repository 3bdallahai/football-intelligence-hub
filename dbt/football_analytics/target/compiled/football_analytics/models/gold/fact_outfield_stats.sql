

WITH outfield AS (

    SELECT *
    FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.stg_players
    WHERE UPPER(TRIM(player_position)) != 'GK'

),

dim_season AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_season

),

dim_player AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_player

),

dim_club AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_club

)

SELECT

    -- ── KEYS FROM DIMENSIONS ────────────────────────────────────────────────
    p.player_key,
    c.club_key,
    s.season_key,

    -- ── DEGENERATE DIMENSIONS ────────────────────────────────────────────────
    o.player_name,
    o.season,
    o.club,
    o.competition,
    o.player_age,

    -- ── PLAYING TIME ─────────────────────────────────────────────────────────
    o.matches_played,
    o.matches_started,
    o.minutes_played,
    o.full_90s_played,
    o.minutes_per_match,
    o.minutes_percentage,
    o.minutes_per_start,
    o.full_matches_completed,
    o.minutes_per_substitution,
    o.unused_substitute_matches,
    o.points_per_match,

    -- ── GOAL CONTRIBUTIONS ───────────────────────────────────────────────────
    o.goals,
    o.assists,
    o.goal_contributions,
    o.non_penalty_goals,
    o.penalty_goals,
    o.penalty_attempts,

    -- ── PER 90 RATES ─────────────────────────────────────────────────────────
    o.goals_per_90,
    o.assists_per_90,
    o.goal_contributions_per_90,
    o.non_penalty_goals_per_90,
    o.non_penalty_goal_contributions_per_90,

    -- ── SHOOTING ─────────────────────────────────────────────────────────────
    o.total_shots,
    o.shots_on_target,
    o.shots_on_target_percentage,
    o.shots_per_90,
    o.shots_on_target_per_90,
    o.goals_per_shot,
    o.goals_per_shot_on_target,

    -- ── TEAM IMPACT ──────────────────────────────────────────────────────────
    o.team_goals_while_on_pitch,
    o.team_goals_conceded_while_on_pitch,
    o.goal_difference_on_pitch,
    o.goal_difference_per_90,
    o.on_off_difference,

    -- ── DEFENSIVE / MISC ─────────────────────────────────────────────────────
    o.fouls_committed,
    o.fouls_drawn,
    o.offsides,
    o.crosses,
    o.interceptions,
    o.tackles_won,
    o.penalties_won,
    o.penalties_conceded,
    o.own_goals,

    -- ── DISCIPLINE ───────────────────────────────────────────────────────────
    o.yellow_cards,
    o.red_cards,

    -- ── SALARY ───────────────────────────────────────────────────────────────
    o.weekly_salary_gbp,
    o.weekly_salary_eur,
    o.weekly_salary_usd,
    o.annual_salary_gbp,
    o.annual_salary_eur,
    o.annual_salary_usd

FROM outfield o

LEFT JOIN dim_season s
    ON s.season_key = 
       CAST(REPLACE(o.season, '-', '') AS INTEGER)

LEFT JOIN dim_player p
    ON p.player_name = o.player_name

LEFT JOIN dim_club c
    ON c.club_name = o.club