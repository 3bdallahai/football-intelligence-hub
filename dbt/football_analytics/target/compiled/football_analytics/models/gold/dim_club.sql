

WITH latest_club AS (

    SELECT *

    FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.stg_players

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY club
        ORDER BY season DESC
    ) = 1

)

SELECT

    MD5(UPPER(TRIM(club))) AS club_key,

    club AS club_name,

    club_country,

    competition

FROM latest_club