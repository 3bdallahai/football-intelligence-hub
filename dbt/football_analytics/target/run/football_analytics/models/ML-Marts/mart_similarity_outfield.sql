
  
    

create or replace transient table FOOTBALL_SILVER.TRANSFORMED.mart_similarity_outfield
    
    
    
    as (

WITH ranked_seasons AS (

    SELECT

        fos.*,

        ROW_NUMBER() OVER (
            PARTITION BY fos.player_key
            ORDER BY fos.season_key DESC
        ) AS season_rank

    FROM FOOTBALL_GOLD.TRANSFORMED_MART.fact_outfield_stats fos

),

weighted AS (

    SELECT

        player_key,

        CASE
            WHEN season_rank = 1 THEN 0.60
            WHEN season_rank = 2 THEN 0.30
            WHEN season_rank = 3 THEN 0.10
            ELSE 0
        END AS season_weight,

        goals_per_90,
        assists_per_90,
        goal_contributions_per_90,

        shots_per_90,
        shots_on_target_per_90,

        goals_per_shot,
        goals_per_shot_on_target,

        interceptions,
        tackles_won,

        yellow_cards,
        red_cards,
        fouls_committed,

        points_per_match,
        goal_difference_per_90,
        on_off_difference,

        minutes_played

    FROM ranked_seasons
    WHERE season_rank <= 3

),


aggregated AS (

    SELECT

        player_key,

        SUM(goals_per_90 * season_weight)
            AS goals_per_90,

        SUM(assists_per_90 * season_weight)
            AS assists_per_90,

        SUM(goal_contributions_per_90 * season_weight)
            AS goal_contributions_per_90,

        SUM(shots_per_90 * season_weight)
            AS shots_per_90,

        SUM(shots_on_target_per_90 * season_weight)
            AS shots_on_target_per_90,

        SUM(goals_per_shot * season_weight)
            AS goals_per_shot,

        SUM(goals_per_shot_on_target * season_weight)
            AS goals_per_shot_on_target,

        SUM(interceptions * season_weight)
            AS interceptions,

        SUM(tackles_won * season_weight)
            AS tackles_won,

        SUM(yellow_cards * season_weight)
            AS yellow_cards,

        SUM(red_cards * season_weight)
            AS red_cards,

        SUM(fouls_committed * season_weight)
            AS fouls_committed,

        SUM(points_per_match * season_weight)
            AS points_per_match,

        SUM(goal_difference_per_90 * season_weight)
            AS goal_difference_per_90,

        SUM(on_off_difference * season_weight)
            AS on_off_difference,

        SUM(minutes_played * season_weight)
            AS minutes_played

    FROM weighted
    GROUP BY player_key

),

latest_market_value AS (

    SELECT *

    FROM (

        SELECT

            player_key,
            market_value_eur,
            age,

            ROW_NUMBER() OVER (
                PARTITION BY player_key
                ORDER BY valuation_date DESC
            ) AS rn

        FROM FOOTBALL_GOLD.TRANSFORMED_MART.fact_market_value_history

    )

    WHERE rn = 1

)
SELECT

    a.player_key,
    dp.player_name,

    dp.primary_position,
    dp.secondary_position,
    dp.position_details,

    mv.age,
    mv.market_value_eur,

    a.goals_per_90,
    a.assists_per_90,
    a.goal_contributions_per_90,
    a.shots_per_90,
    a.shots_on_target_per_90,
    a.goals_per_shot,
    a.goals_per_shot_on_target,
    a.interceptions,
    a.tackles_won,
    a.yellow_cards,
    a.red_cards,
    a.fouls_committed,
    a.points_per_match,
    a.goal_difference_per_90,
    a.on_off_difference,
    a.minutes_played

FROM aggregated a

JOIN FOOTBALL_GOLD.TRANSFORMED_MART.dim_player dp
    ON a.player_key = dp.player_key

LEFT JOIN latest_market_value mv
    ON a.player_key = mv.player_key
    )
;


  