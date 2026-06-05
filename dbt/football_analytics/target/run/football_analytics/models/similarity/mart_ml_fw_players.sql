
  
    

create or replace transient table FOOTBALL_GOLD.TRANSFORMED_SIMILARITY.mart_ml_fw_players
    
    
    
    as (

WITH base AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.fact_outfield_stats
    JOIN dim_player dp
    ON fact.player_key = dp.player_key
    WHERE dp.primary_position = 'FW'
),

season_ranked AS (

    SELECT *,
           MAX(season_key) OVER () AS max_season
    FROM base

),

weighted AS (

    SELECT
        player_key,
        player_name,
        player_position,
        season_key,

        /* recency weight (IMPORTANT for decay model) */
        POWER(0.7, (max_season - season_key)) AS recency_weight,

        /* ATTACK FEATURES */
        goals_per_90,
        assists_per_90,
        shots_per_90,
        shots_on_target_per_90,
        goals_per_shot,

        /* SUPPORT FEATURES */
        passes_attempted,
        pass_accuracy,
        crosses,
        fouls_drawn,

        /* DISCIPLINE */
        yellow_cards,
        red_cards,

        /* PHYSICAL / USAGE */
        minutes_played,
        minutes_percentage,

        market_value_eur

    FROM season_ranked

),

final AS (

    SELECT
        player_key,

        /* aggregated weighted features */
        SUM(goals_per_90 * recency_weight)        AS goals_per_90,
        SUM(assists_per_90 * recency_weight)      AS assists_per_90,
        SUM(shots_per_90 * recency_weight)        AS shots_per_90,
        SUM(shots_on_target_per_90 * recency_weight) AS shots_on_target_per_90,
        SUM(goals_per_shot * recency_weight)      AS goals_per_shot,

        SUM(crosses * recency_weight)             AS crosses,
        SUM(fouls_drawn * recency_weight)         AS fouls_drawn,

        SUM(yellow_cards * recency_weight)        AS yellow_cards,
        SUM(red_cards * recency_weight)           AS red_cards,

        SUM(minutes_played * recency_weight)      AS minutes_played,

        MAX(market_value_eur) AS market_value_eur

    FROM weighted
    GROUP BY player_key

)

SELECT * FROM final
    )
;


  