
  
    

create or replace transient table FOOTBALL_GOLD.TRANSFORMED_MART.fact_market_value_history
    
    
    
    as (

WITH market_values AS (

    SELECT *
    FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.stg_market_values

),

bridge AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_player_bridge

),

dim_player AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_player

),

dim_season AS (

    SELECT *
    FROM FOOTBALL_GOLD.TRANSFORMED_MART.dim_season

)

SELECT

    dp.player_key,

    ds.season_key,

    mv.player_name AS market_player_name,

    COALESCE(
        b.canonical_player_name,
        mv.player_name
    ) AS canonical_player_name,

    mv.club,

    mv.valuation_date,

    mv.age,

    mv.market_value_eur,

    b.name_sim,
    b.team_sim

FROM market_values mv

LEFT JOIN bridge b
    ON mv.player_name = b.player_from_market

LEFT JOIN dim_player dp
    ON COALESCE(
           b.canonical_player_name,
           mv.player_name
       ) = dp.player_name

LEFT JOIN dim_season ds
    ON mv.season = ds.season
    )
;


  