CREATE PIPELINE `events_pipeline`
AS LOAD DATA KAFKA 'ec2_ip:9092/match.events'
BATCH_INTERVAL 1
DISABLE OUT_OF_ORDER OPTIMIZATION
DISABLE OFFSETS METADATA GC
SKIP DUPLICATE KEY ERRORS
INTO TABLE `match_events_live`
FORMAT JSON
(
    `match_events_live`.`id` <- `id`,
    `match_events_live`.`event_id` <- `event_id`,
    `match_events_live`.`minute` <- `minute`,
    `match_events_live`.`second` <- `second`,
    `match_events_live`.`team_id` <- `team_id`,
    `match_events_live`.`team_name` <- `team_name`,
    `match_events_live`.`player_id` <- `player_id`,
    `match_events_live`.`player_name` <- `player_name`,
    `match_events_live`.`x` <- `x`,
    `match_events_live`.`y` <- `y`,
    `match_events_live`.`end_x` <- `end_x`,
    `match_events_live`.`end_y` <- `end_y`,
    `match_events_live`.`is_touch` <- `is_touch`,
    `match_events_live`.`is_shot` <- `is_shot`,
    `match_events_live`.`is_goal` <- `is_goal`,
    `match_events_live`.`card_type` <- `card_type`,
    `match_events_live`.`type_display_name` <- `type_display_name`,
    `match_events_live`.`outcome_type_display_name` <- `outcome_type_display_name`,
    `match_events_live`.`period_display_name` <- `period_display_name`,
    `match_events_live`.`match_url` <- `match_url`,
    `match_events_live`.`event_timestamp` <- `event_timestamp`
)