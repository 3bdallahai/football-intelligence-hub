/*
  stg_market_values
  Source: FOOTBALL_BRONZE.RAW.BRONZE_MARKET_VALUES
  What we do here:
    - Cast VARCHAR columns to correct types
    - Filter out any null player names
    - Keep all 51,112 rows as-is (already clean, no deduplication needed)
*/

WITH source AS (

    SELECT * FROM {{ source('bronze', 'BRONZE_MARKET_VALUES') }}

),

cast_types AS (

    SELECT

        player_name,

        -- player_id was "315858" as text → now a real integer
        TRY_CAST(player_id AS INTEGER)          AS player_id,

        league,
        club,

        -- date was "2015-11-05" as text → now a real date
        TRY_TO_DATE(date, 'YYYY-MM-DD')         AS valuation_date,

        -- age was "16" as text → now a real integer
        TRY_CAST(age AS INTEGER)                AS age,

        -- market_value_eur was "1500000.0" as text → now a real number
        TRY_CAST(market_value_eur AS FLOAT)     AS market_value_eur,

        season,

        -- keep metadata for traceability
        _source_file,
        _load_timestamp

    FROM source

    -- Filter out any completely empty rows
    WHERE player_name IS NOT NULL

)

SELECT * FROM cast_types