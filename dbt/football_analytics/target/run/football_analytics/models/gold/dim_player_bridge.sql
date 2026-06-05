
  
    

create or replace transient table FOOTBALL_GOLD.TRANSFORMED_MART.dim_player_bridge
    
    
    
    as (

SELECT DISTINCT

    MD5(
        UPPER(
            TRIM(player_from_players)
        )
    ) AS player_key,

    PLAYER_FROM_PLAYERS AS canonical_player_name,

    PLAYER_FROM_MARKET,

    PLAYER_TEAM,

    MARKET_TEAM,

    NAME_SIM,

    TEAM_SIM

FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.int_player_name_match

WHERE NAME_SIM > 87
  AND TEAM_SIM > 85
    )
;


  