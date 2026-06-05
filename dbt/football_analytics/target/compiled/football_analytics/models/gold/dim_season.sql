

WITH source AS (

    SELECT DISTINCT
        season
    FROM FOOTBALL_SILVER.TRANSFORMED_TRANSFORMED.stg_players

),

parsed AS (

    SELECT

        season,

        /* -----------------------------
           Detect start & end year
        ----------------------------- */
        TRY_CAST(SPLIT_PART(season, '-', 1) AS INTEGER) AS season_start_year,

        CASE 
            WHEN season LIKE '%-%'
                THEN TRY_CAST(SPLIT_PART(season, '-', 2) AS INTEGER)
            ELSE TRY_CAST(SPLIT_PART(season, '-', 1) AS INTEGER)
        END AS season_end_year

    FROM source

),

final AS (

    SELECT

        /* -----------------------------
           KEY: 20232024 or 2023
        ----------------------------- */
        CASE 
            WHEN season_start_year IS NOT NULL 
                 AND season_end_year IS NOT NULL
                 AND season_start_year <> season_end_year
            THEN TO_VARCHAR(season_start_year) || TO_VARCHAR(season_end_year)
            ELSE TO_VARCHAR(season_start_year)
        END AS season_key,

        season,
        season_start_year,
        season_end_year,

        CASE 
            WHEN season_start_year = season_end_year THEN 1
            ELSE 0
        END AS is_single_year

    FROM parsed

)

SELECT *
FROM final