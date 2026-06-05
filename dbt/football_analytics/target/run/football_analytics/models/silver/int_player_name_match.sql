
  
    

create or replace transient table FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.int_player_name_match
    
    
    
    as (WITH players AS (

    SELECT DISTINCT
        player_name,
        current_club AS player_team,

        -- SAFE CLEANING (NO UNICODE_NORMALIZE)
       LOWER(
    REGEXP_REPLACE(
        TRANSLATE(player_name,
            'áàäâãåāçćčďéèëêěíìïîłńñóòöôøřšşśťúùüûýÿž',
            'aaaaaaaaccceeeeeiiiilnnooooorssstuuuuyyz'
        ),
        '[^A-Za-z0-9 ]',
        ''
    )
) AS p_clean

    FROM FOOTBALL_BRONZE.RAW.BRONZE_PLAYERS

),

market AS (

    SELECT DISTINCT
        player_name,
        club AS market_team,

        LOWER(
    REGEXP_REPLACE(
        TRANSLATE(player_name,
            'áàäâãåāçćčďéèëêěíìïîłńñóòöôøřšşśťúùüûýÿž',
            'aaaaaaaaccceeeeeiiiilnnooooorssstuuuuyyz'
        ),
        '[^A-Za-z0-9 ]',
        ''
    )
) AS m_clean

    FROM FOOTBALL_BRONZE.RAW.BRONZE_MARKET_VALUES

),

pairs AS (

    SELECT

        p.player_name AS player_from_players,
        m.player_name AS player_from_market,

        p.player_team,
        m.market_team,

        p.p_clean,
        m.m_clean

    FROM players p
    CROSS JOIN market m

    -- blocking for performance
    WHERE LEFT(p.p_clean, 3) = LEFT(m.m_clean, 3)

),

scored AS (

    SELECT *,

        -- name similarity
        JAROWINKLER_SIMILARITY(p_clean, m_clean) AS name_sim,

        -- exact team match
        CASE
            WHEN LOWER(COALESCE(player_team, ''))
               = LOWER(COALESCE(market_team, ''))
            THEN 1 ELSE 0
        END AS team_match,

        -- soft team similarity
        JAROWINKLER_SIMILARITY(
            LOWER(COALESCE(player_team, '')),
            LOWER(COALESCE(market_team, ''))
        ) AS team_sim

    FROM pairs

),

final_scoring AS (

    SELECT *,

        (
            name_sim * 0.75 +
            team_sim * 0.15 +
            team_match * 0.10
        ) AS final_score

    FROM scored

),

ranked AS (

    SELECT *,

        ROW_NUMBER() OVER (
            PARTITION BY player_from_players
            ORDER BY final_score DESC
        ) AS rn

    FROM final_scoring

)

SELECT
    player_from_players,
    player_from_market,

    player_team,
    market_team,

    p_clean,
    m_clean,

    name_sim,
    team_sim,
    team_match,
    final_score

FROM ranked

WHERE rn = 1
    )
;


  