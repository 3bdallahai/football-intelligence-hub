
  
    

create or replace transient table FOOTBALL_GOLD.TRANSFORMED_MART.dim_name_bridge
    
    
    
    as (

SELECT *
FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.int_player_name_match
LIMIT 10
    )
;


  