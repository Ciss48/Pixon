-- raw 
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.raw`
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
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));


-- ad_impression
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_format') as ad_format,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') as value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_source') as ad_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ad_platform') as ad_platform, 
    platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'ad_impression' and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

Ư
-- first_open
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.first_open`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    advertising_id
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'first_open'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);



-- session_start
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.session_start`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'session_start'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);



-- user_engagement
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.user_engagement`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec, 
    platform
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'user_engagement'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- screen_view
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.screen_view`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'screen_view'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);


-- play_chapter
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.play_chapter`
SELECT
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
WHERE event_name = 'play_chapter'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);  




-- start_chapter_wave
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.start_chapter_wave`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'start_chapter_wave'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);


-- lose_chapter_wave
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.lose_chapter_wave`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'lose_chapter'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);


-- win_chapter
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter_wave') AS chapter_wave,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_number') AS ga_session_number
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'win_chapter'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);




-- af_rewarded
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.af_rewarded`
SELECT
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
WHERE event_name = 'af_rewarded'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);


-- in_app_purchase
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
SELECT
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
WHERE event_name = 'in_app_purchase'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);




-- revive_chapter
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.revive_chapter`
SELECT
    event_timestamp,
    user_pseudo_id,
    event_date,
    version,
    country,
    platform,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'chapter') AS chapter,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'wave') AS wave
FROM `bounce-survivor---puzzle-rpg.flatten_table.raw`
WHERE event_name = 'revive_chapter'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);  



-- join_chapter
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
SELECT
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
WHERE event_name = 'join_chapter'and event_date  = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);  







-- iap_chapter
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.iap_chapter`
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
  WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
first_open_dates AS (
  SELECT
    user_pseudo_id,
    MIN(event_date) AS first_open_date
  FROM `bounce-survivor---puzzle-rpg.flatten_table.first_open`
  WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
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
INSERT INTO `bounce-survivor---puzzle-rpg.flatten_table.win_chapter_hero`
WITH join_chapter AS (
  SELECT
    user_pseudo_id,
    platform,
    version,
    chapter,
    event_timestamp,
    hero
  FROM `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
  WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
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
  WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
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
