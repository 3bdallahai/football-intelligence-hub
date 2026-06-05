{{ config(
    materialized='table'
) }}

WITH latest_player AS (

    SELECT *

    FROM {{ ref('stg_players') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_name
        ORDER BY season DESC
    ) = 1

),

bridge AS (

    SELECT *
    FROM {{ ref('dim_player_bridge') }}

),

parsed AS (

    SELECT
        lp.*,

        b.canonical_player_key,

        UPPER(TRIM(lp.player_position))                                              AS pos_clean,

        REGEXP_SUBSTR(
            UPPER(TRIM(lp.player_position)),
            '\\((.*?)\\)',
            1,
            1,
            'e',
            1
        ) AS raw_details

    FROM latest_player lp

    LEFT JOIN bridge b
        ON lp.player_name = b.canonical_player_name

)

SELECT

    COALESCE(
        canonical_player_key,
        MD5(UPPER(TRIM(player_name)))
    ) AS player_key,

    player_name,

    preferred_foot,

    national_team,

    height_cm,

    weight_kg,

    /* ─────────────────────────
       PRIMARY POSITION
    ───────────────────────── */
    CASE
        WHEN pos_clean LIKE '%GK%' THEN 'GK'
        WHEN pos_clean LIKE '%DF%' THEN 'DF'
        WHEN pos_clean LIKE '%MF%' THEN 'MF'
        WHEN pos_clean LIKE '%FW%' THEN 'FW'
        ELSE 'UNKNOWN'
    END AS primary_position,

    /* ─────────────────────────
       SECONDARY POSITION
    ───────────────────────── */
    CASE
        WHEN pos_clean LIKE '%GK%' THEN NULL

        WHEN pos_clean LIKE '%DF%'
         AND pos_clean LIKE '%MF%' THEN 'MF'

        WHEN pos_clean LIKE '%DF%'
         AND pos_clean LIKE '%FW%' THEN 'FW'

        WHEN pos_clean LIKE '%MF%'
         AND pos_clean LIKE '%FW%' THEN 'FW'

        ELSE NULL
    END AS secondary_position,

    /* ─────────────────────────
       POSITION DETAILS
    ───────────────────────── */
    NULLIF(
        REPLACE(
            REPLACE(
                REPLACE(
                    COALESCE(raw_details, ''),
                    ',',
                    '|'
                ),
                ' ',
                ''
            ),
            '-',
            '|'
        ),
        ''
    ) AS position_details

FROM parsed