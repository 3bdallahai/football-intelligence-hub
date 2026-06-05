

WITH ranked_seasons AS (

    SELECT

        fgs.*,

        ROW_NUMBER() OVER (
            PARTITION BY fgs.player_key
            ORDER BY fgs.season_key DESC
        ) AS season_rank

    FROM FOOTBALL_GOLD.TRANSFORMED_MART.fact_goalkeeper_stats fgs

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

        save_percentage,
        goals_conceded_per_90,
        clean_sheet_percentage,

        shots_on_target_against,
        saves,

        wins,
        draws,
        losses,

        penalties_faced,
        penalty_goals_conceded,

        yellow_cards,
        red_cards,
        fouls_committed,

        minutes_played

    FROM ranked_seasons
    WHERE season_rank <= 3

),

aggregated AS (

    SELECT

        player_key,

        SUM(save_percentage * season_weight)
            AS save_percentage,

        SUM(goals_conceded_per_90 * season_weight)
            AS goals_conceded_per_90,

        SUM(clean_sheet_percentage * season_weight)
            AS clean_sheet_percentage,

        SUM(shots_on_target_against * season_weight)
            AS shots_on_target_against,

        SUM(saves * season_weight)
            AS saves,

        SUM(wins * season_weight)
            AS wins,

        SUM(draws * season_weight)
            AS draws,

        SUM(losses * season_weight)
            AS losses,

        SUM(penalties_faced * season_weight)
            AS penalties_faced,

        SUM(penalty_goals_conceded * season_weight)
            AS penalty_goals_conceded,

        SUM(yellow_cards * season_weight)
            AS yellow_cards,

        SUM(red_cards * season_weight)
            AS red_cards,

        SUM(fouls_committed * season_weight)
            AS fouls_committed,

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

    dp.player_key,
    dp.player_name,

    dp.primary_position,
    dp.secondary_position,
    dp.position_details,

    mv.age,
    mv.market_value_eur,

    a.*

FROM aggregated a

JOIN FOOTBALL_GOLD.TRANSFORMED_MART.dim_player dp
    ON a.player_key = dp.player_key

LEFT JOIN latest_market_value mv
    ON a.player_key = mv.player_key