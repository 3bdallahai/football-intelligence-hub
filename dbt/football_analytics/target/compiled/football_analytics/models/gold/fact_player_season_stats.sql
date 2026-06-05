

WITH players AS (

    SELECT *
    FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.stg_players

),

dim_season AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_season

)

SELECT

    MD5(UPPER(TRIM(player_name))) AS player_key,

    MD5(UPPER(TRIM(club))) AS club_key,

    ds.season_key,

    player_name,
    club,
    competition,

    player_age,

    matches_played,
    matches_started,
    minutes_played,
    full_90s_played,

    goals,
    assists,
    goal_contributions,

    goals_per_90,
    assists_per_90,
    goal_contributions_per_90,

    total_shots,
    shots_per_90,

    interceptions,
    tackles_won,
    crosses,

    yellow_cards,
    red_cards,

    weekly_salary_eur,
    annual_salary_eur

FROM players p

LEFT JOIN dim_season ds
    ON p.season = ds.season