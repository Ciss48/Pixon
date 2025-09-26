-- raw 
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.raw`
PARTITION BY event_date
CLUSTER BY event_name
AS
SELECT 
  event_timestamp,
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_name,
  user_pseudo_id,
  event_params,           -- giữ nguyên array
  event_value_in_usd,
  app_info.version AS version,
  geo.country AS country,
  platform,
  device.advertising_id
FROM `bounce-survivor---puzzle-rpg.analytics_464269801.events_intraday_*`
WHERE _TABLE_SUFFIX BETWEEN '20250720' AND '20250818';



-- ad_impression
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_format') as ad_format,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') as value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_source') as ad_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_platform') as ad_platform, platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'ad_impression';


-- first_open
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.first_open`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    advertising_id
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'first_open';


-- app_remove
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.app_remove`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'app_remove';


-- session_start
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.session_start`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'session_start';

-- user_engagement
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.user_engagement`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec, platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'user_engagement';

-- screen_view
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.screen_view`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'screen_view';


-- play_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.play_chapter`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'play_chapter';  



-- start_chapter_wave
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.start_chapter_wave`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter_wave
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'start_chapter_wave';


-- lose_chapter_wave
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.lose_chapter_wave`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter_wave
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'lose_chapter';


-- win_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter_wave
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'win_chapter';






-- af_inters
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.af_inters`
PARTITION BY event_date
CLUSTER BY version, country, platform AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'af_inters';


-- af_rewarded
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.af_rewarded`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter_wave
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'reward_type') AS reward_type,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'af_rewarded';


-- in_app_purchase
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'product_id') AS product_id,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'product_name') AS product_name,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'quantity') AS quantity,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'validated') AS validated,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
    event_value_in_usd
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'in_app_purchase';



-- IAP_home_click
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.IAP_home_click`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'error_value') AS error_value,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
    event_value_in_usd
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'IAP_home_click';


-- popup_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.popup_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'error_value') AS error_value,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
    event_value_in_usd
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'popup_show';


-- quit_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.quit_chapter`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'quit_placement') AS quit_placement,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'quit_chapter';  



-- puzzle_map_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.puzzle_map_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'puzzle_map_show') AS puzzle_map
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'puzzle_map_show'; 


-- puzzle_map_complete
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.puzzle_map_complete`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ball_cup') AS ball_cup,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'starting_ball') AS starting_ball,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'collected_ball') AS collected_ball,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ending_ball') AS ending_ball,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'puzzle_name') AS puzzle_name,
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'puzzle_map_complete';  


-- speed_up_btn_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.speed_up_btn_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'speed_up_btn_interact'; 


-- pause_btn_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.pause_btn_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'pause_btn_interact'; 


-- pause_btn_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.pause_btn_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'pause_btn_interact'; 


-- skill_buy
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.skill_buy`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'skill') AS skill,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'buy_method') AS buy_method
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'skill_buy';  


-- join_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'atk') AS atk,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'hp') AS hp,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'def') AS def,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'hero') AS hero,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'blessing_hp') AS blessing_hp,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'blessing_atk') AS blessing_atk,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'blessing_def') AS blessing_def
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'join_chapter';  


-- revive_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.revive_chapter`
PARTITION BY event_date
CLUSTER BY version, country, platform, chapter
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'revive_chapter';  


-- daily_quest_home_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.daily_quest_home_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'daily_quest_home_interact';  


-- daily_quest_complete
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.daily_quest_complete`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'quest') AS quest,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'daily_quest_complete';  


-- daily_quest_full_complete
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.daily_quest_full_complete`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'daily_quest_full_complete';  


-- daily_quest_popup_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.daily_quest_popup_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'daily_quest_popup_show';  




-- battle_pass_home_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.battle_pass_home_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'battle_pass_home_interact'; 


-- battle_pass_complete_level
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.battle_pass_complete_level`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'milestone') AS milestone
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'battle_pass_complete_level'; 



-- battle_pass_popup_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.battle_pass_popup_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'battle_pass_popup_show'; 


-- battle_pass_skip_milestone
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.battle_pass_skip_milestone`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'milestone') AS milestone
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'battle_pass_skip_milestone'; 



-- blessing_home_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.blessing_home_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'blessing_home_interact'; 



-- blessing_popup_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.blessing_popup_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'blessing_popup_show'; 


-- blessing_buff_gain
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.blessing_buff_gain`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'buff') AS buff
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'blessing_buff_gain'; 



-- blessing_full_buff_complete
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.blessing_full_buff_complete`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'blessing_full_buff_complete'; 



-- talent_tree_home_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.talent_tree_home_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'talent_tree_home_interact'; 



-- talent_tree_popup_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.talent_tree_popup_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'talent_tree_popup_show'; 



-- talent_tree_level_reach
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.talent_tree_level_reach`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'level') AS level,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'stat_type') AS stat_type
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'talent_tree_level_reach'; 


-- talent_tree_upgrade_behavior
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.talent_tree_upgrade_behavior`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'talent_tree_upgrade_behavior'; 


-- shop_screen_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.shop_screen_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'shop_screen_show'; 



-- shop_icon_interact
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.shop_icon_interact`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'shop_icon_interact'; 




-- shop_chest_open_key
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.shop_chest_open_key`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chest_reward') AS chest_reward,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'type') AS type,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'shop_chest_open_key'; 



-- shop_diamond_buy
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.shop_diamond_buy`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'pack') AS pack,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'shop_diamond_buy'; 



-- equipment_screen_show
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.equipment_screen_show`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'pack') AS pack,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'equipment_screen_show'; 



-- hero_upgrade
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.hero_upgrade`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'hero_name') AS hero_name,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'hero_level') AS hero_level
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'hero_upgrade'; 



-- hero_skill_upgrade
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.hero_skill_upgrade`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'hero_name') AS hero_name,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'skill_name') AS skill_name
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'hero_skill_upgrade'; 





-- scavenge_beyond_start
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.scavenge_beyond_start`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    'ticket' as start_type
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'scavenge_beyond_start_using_ticket'
UNION ALL
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    'ads' as start_type
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'scavenge_beyond_start_using_ads';



-- unlock_scavenge_beyond_feature
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.unlock_scavenge_beyond_feature`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    'ticket' as start_type
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'unlock_scavenge_beyond_feature';





-- event_7days_reward_free_claim
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.event_7days_reward_free_claim`
PARTITION BY event_date
CLUSTER BY version, country, platform
AS
SELECT DISTINCT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'event_day') AS event_day
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'event_7days_reward_free_claim';








-- iap_chapter
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.iap_chapter`
PARTITION BY event_date
CLUSTER BY chapter, version, platform AS
WITH chapter_ranges AS (
  SELECT 
    user_pseudo_id,
    chapter_wave,
    version,
    country,
    platform,
    event_timestamp AS chapter_start_time,
    LEAD(event_timestamp) OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY event_timestamp
    ) AS chapter_end_time
  FROM `bounce-survivor---puzzle-rpg.flatten_table.start_chapter_wave`
),
first_open_dates AS (
  SELECT
    user_pseudo_id,
    MIN(event_date) AS first_open_date
  FROM `bounce-survivor---puzzle-rpg.flatten_table.first_open`
  GROUP BY user_pseudo_id
)
SELECT
  iap.event_timestamp,
  iap.event_date,
  iap.user_pseudo_id,
  iap.country,
  iap.version,
  iap.platform,
  iap.product_id,
  REPLACE(REPLACE(iap.product_id, 'com.zombie.cup.puzzle_', ''), 'com.hero.zombie.cup.puzzle_', '') AS product_name,
  iap.event_value_in_usd,
  lr.chapter_wave,
  CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) AS chapter,
  DATE_DIFF(iap.event_date, fod.first_open_date, DAY) AS Day_since_fo,
  -- Day_group logic
  CASE
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) = 0 THEN 'Day0'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) BETWEEN 1 AND 3 THEN 'Day01_03'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) BETWEEN 3 AND 7 THEN 'Day03_07'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) BETWEEN 8 AND 14 THEN 'Day08_14'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) BETWEEN 15 AND 21 THEN 'Day15_21'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) BETWEEN 22 AND 29 THEN 'Day21_29'
    WHEN DATE_DIFF(iap.event_date, fod.first_open_date, DAY) >= 30 THEN 'Day30_'
    ELSE NULL
  END AS Day_group,
  CASE 
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 1 and 10 then '1_10'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 11 and 20 then '11_20'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 21 and 30 then '21_30'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 31 and 40 then '31_40'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 41 and 50 then '41_50'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) between 51 and 100 then '51_100'
    WHEN CAST(SPLIT(lr.chapter_wave, "_")[OFFSET(0)] AS INT64) > 100 then '100+' 
    ELSE NULL 
  END AS Chapter_group
FROM `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase` iap
LEFT JOIN chapter_ranges lr
  ON iap.user_pseudo_id = lr.user_pseudo_id
  AND iap.event_timestamp >= lr.chapter_start_time 
  AND (iap.event_timestamp < lr.chapter_end_time OR lr.chapter_end_time IS NULL)
LEFT JOIN first_open_dates fod
  ON iap.user_pseudo_id = fod.user_pseudo_id;

















-- win_chapter_hero
CREATE OR REPLACE TABLE `bounce-survivor---puzzle-rpg.flatten_table.win_chapter_hero`
PARTITION BY event_date
CLUSTER BY chapter, version, platform AS
WITH join_chapter AS (
  SELECT
    user_pseudo_id,
    platform,
    version,
    chapter,
    event_timestamp,
    hero
  FROM `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
),
win_chapter AS (
  SELECT
    user_pseudo_id,
    platform,
    version,
    CAST(SPLIT(chapter_wave, "_")[OFFSET(0)] AS INT64) AS chapter,
    event_timestamp AS win_time,
    event_date
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
),
joined AS (
  SELECT
    w.event_date,
    w.win_time,
    w.user_pseudo_id,
    w.platform,
    w.version,
    w.chapter,
    j.hero,
    j.event_timestamp AS join_time
  FROM win_chapter w
  LEFT JOIN join_chapter j
    ON w.user_pseudo_id = j.user_pseudo_id
   AND w.platform = j.platform
   AND w.version = j.version
   AND w.chapter = j.chapter
   AND j.event_timestamp <= w.win_time
)
SELECT
  event_date,
  win_time AS event_timestamp,
  user_pseudo_id,
  platform,
  version,
  chapter,
  hero
FROM joined
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY user_pseudo_id, platform, version, chapter, win_time
  ORDER BY join_time DESC
) = 1;












