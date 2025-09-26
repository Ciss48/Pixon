-- dau, iap, ad new 
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.dau_new_iap`
with dau as 
(
  select event_date, version, platform, country,
  count(distinct user_pseudo_id) as dau
  from  `bounce-survivor---puzzle-rpg.flatten_table.user_engagement`
  group by 1,2,3,4 
),
new_user as 
(
  select event_date, version, platform, country,
  count(distinct user_pseudo_id) as new_user
  from  `bounce-survivor---puzzle-rpg.flatten_table.first_open`
  group by 1,2,3,4 
),
iap as 
(
  select event_date, version, platform, country,
  sum(quantity) as num_iap,
  sum(event_value_in_usd) as iap_revenue 
  from `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
  group by 1,2,3,4
),
ad as 
(
  select event_date, version, platform, country,
  sum(value) as ad_revenue 
  from `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
  group by 1,2,3,4
)
select 
  a.event_date, 
  a.version, 
  a.platform,
  a.country,
  a.dau,
  b.new_user,
  c.num_iap,
  c.iap_revenue,
  d.ad_revenue,
  CASE 
    WHEN d.ad_revenue IS NOT NULL THEN 'Ad Revenue'
    WHEN c.iap_revenue IS NOT NULL THEN 'IAP Revenue'
  END as dimension
from dau a
left join new_user b on a.event_date = b.event_date 
  and a.version = b.version and a.platform = b.platform and a.country = b.country
left join iap c on a.event_date = c.event_date 
  and a.version = c.version and a.platform = c.platform and a.country = c.country
left join ad d on a.event_date = d.event_date 
  and a.version = d.version and a.platform = d.platform and a.country = d.country
where a.event_date between '2025-09-04' and '2025-09-07';



-- retention (khó cập nhật daily)
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.retention`
with user_d0 as 
(
  select distinct 
    user_pseudo_id, event_date, platform, country, version 
  from `bounce-survivor---puzzle-rpg.flatten_table.first_open`
),
user_active as 
(
  select distinct 
    user_pseudo_id, event_date, platform, country, version 
  from `bounce-survivor---puzzle-rpg.flatten_table.user_engagement`
), 
d0 as 
(
  select event_date, platform, country, version,
    count(distinct user_pseudo_id) as new_user 
  from `bounce-survivor---puzzle-rpg.flatten_table.first_open`
  group by 1,2,3,4
),
d1 as  
(
  select a.event_date, a.platform, a.country, a.version,
    count(distinct a.user_pseudo_id) as user_retained_d1
  from
    user_d0 a join user_active b
      on a.user_pseudo_id = b.user_pseudo_id 
      and a.event_date + 1 = b.event_date
  group by 1,2,3,4    
),
d3 as 
(
  select a.event_date, a.platform, a.country, a.version,
    count(distinct  a.user_pseudo_id) as user_retained_d3 
  from
    user_d0 a join user_active b
      on a.user_pseudo_id = b.user_pseudo_id 
      and a.event_date + 3 = b.event_date
  group by 1,2,3,4       
),
d7 as
(
  select a.event_date, a.platform, a.country, a.version,
    count(distinct  a.user_pseudo_id) as user_retained_d7
  from
    user_d0 a join user_active b
      on a.user_pseudo_id = b.user_pseudo_id 
      and a.event_date + 7 = b.event_date
  group by 1,2,3,4         
)
select 
  d0.event_date, 
  d0.platform,
  d0.country,
  d0.version,
  d0.new_user,
  d1.user_retained_d1, d3.user_retained_d3, d7.user_retained_d7
from d0 
left join d1 on d0.event_date = d1.event_date and d0.platform = d1.platform and d0.country = d1.country and d0.version = d1.version
left join d3 on d0.event_date = d3.event_date and d0.platform = d3.platform and d0.country = d3.country and d0.version = d3.version
left join d7 on d0.event_date = d7.event_date and d0.platform = d7.platform and d0.country = d7.country and d0.version = d7.version
where d0.event_date between '2025-09-04' and '2025-09-07';





-- playtime
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.playtime`
WITH screen_view_agg AS (
  SELECT 
    version,
    event_date,
    country,
    SUM(engagement_time_msec) as screen_view_time
  FROM `bounce-survivor---puzzle-rpg.flatten_table.screen_view`
  GROUP BY version, event_date, country
),
user_engagement_agg AS (
  SELECT 
    version,
    event_date,
    country,
    COUNT(DISTINCT user_pseudo_id) as num_user,
    COUNT(DISTINCT ga_session_id) as total_session,
    SUM(engagement_time_msec) as engagement_time
  FROM `bounce-survivor---puzzle-rpg.flatten_table.user_engagement`
  GROUP BY version, event_date, country
)
SELECT 
  ue.version as version,
  ue.event_date as event_date,
  ue.country as country,
  ue.num_user,
  ue.total_session,
  ROUND((COALESCE(ue.engagement_time, 0) + COALESCE(sv.screen_view_time, 0)) / 60000, 2) as total_session_length_minute
FROM user_engagement_agg ue 
FULL OUTER JOIN screen_view_agg sv
  ON ue.version = sv.version
  AND ue.event_date = sv.event_date
  AND ue.country = sv.country
WHERE ue.event_date between '2025-09-04' and '2025-09-07';




-- rev_type
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.rev_type`
WITH ad_inter AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    'ad_inter' AS type,
    SUM(value) AS revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
  WHERE ad_format = 'INTER'
  GROUP BY event_date, country, platform, version
),
ad_rewarded AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    'ad_rewarded' AS type,
    SUM(value) AS revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
  WHERE ad_format = 'REWARDED'
  GROUP BY event_date, country, platform, version
),
in_app_purchase AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    'in_app_purchase' AS type,
    SUM(event_value_in_usd) AS revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
  GROUP BY event_date, country, platform, version
)
SELECT * FROM ad_inter
WHERE event_date between '2025-09-04' and '2025-09-07'
UNION ALL
SELECT * FROM ad_rewarded
WHERE event_date between '2025-09-04' and '2025-09-07'
UNION ALL
SELECT * FROM in_app_purchase
WHERE event_date between '2025-09-04' and '2025-09-07';



-- compare_version 
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.compare_version`
WITH sessions AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    COUNT(DISTINCT user_pseudo_id) AS num_user
  FROM `bounce-survivor---puzzle-rpg.flatten_table.session_start`
  GROUP BY event_date, country, platform, version
),
ad_reward AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    COUNT(*) AS ad_reward_count,
    SUM(value) AS ad_reward_revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
  WHERE ad_format = 'REWARDED'
  GROUP BY event_date, country, platform, version
),
ad_total AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    SUM(value) AS ad_total_revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.ad_impression`
  GROUP BY event_date, country, platform, version
),
iap AS (
  SELECT
    event_date,
    country,
    platform,
    version,
    COUNT(*) AS iap_count,
    SUM(event_value_in_usd) AS iap_revenue
  FROM `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
  GROUP BY event_date, country, platform, version
)
SELECT
  s.event_date,
  s.country,
  s.platform,
  s.version,
  s.num_user,
  IFNULL(a.ad_reward_count, 0) AS ad_reward_count,
  IFNULL(a.ad_reward_revenue, 0) AS ad_reward_revenue,
  IFNULL(t.ad_total_revenue, 0) AS ad_total_revenue,
  IFNULL(i.iap_count, 0) AS iap_count,
  IFNULL(i.iap_revenue, 0) AS iap_revenue,
  (IFNULL(t.ad_total_revenue, 0) + IFNULL(i.iap_revenue, 0)) AS revenue
FROM sessions s
LEFT JOIN ad_reward a
  ON s.event_date = a.event_date
  AND s.country = a.country
  AND s.platform = a.platform
  AND s.version = a.version
LEFT JOIN ad_total t
  ON s.event_date = t.event_date
  AND s.country = t.country
  AND s.platform = t.platform
  AND s.version = t.version
LEFT JOIN iap i
  ON s.event_date = i.event_date
  AND s.country = i.country
  AND s.platform = i.platform
  AND s.version = i.version
WHERE s.event_date between '2025-09-04' and '2025-09-07';




-- iap_sequence
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.iap_sequence`
WITH iap_sequence AS (
  SELECT
    user_pseudo_id,
    event_date,
    platform,
    country,
    version,
    REPLACE(REPLACE(product_id, 'com.zombie.cup.puzzle_', ''), 'com.hero.zombie.cup.puzzle_', '') AS product_name,
    event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, event_date ORDER BY event_timestamp) AS rn
  FROM `bounce-survivor---puzzle-rpg.flatten_table.in_app_purchase`
),
iap_pairs AS (
  SELECT
    a.event_date,
    a.platform,
    a.country,
    a.version,
    a.user_pseudo_id,
    CONCAT('Step_', a.rn, '_', a.product_name) AS source,
    CONCAT('Step_', b.rn, '_', b.product_name) AS target
  FROM iap_sequence a
  JOIN iap_sequence b
    ON a.user_pseudo_id = b.user_pseudo_id
    AND a.event_date = b.event_date
    AND b.rn = a.rn + 1  -- cặp liền kề
)
SELECT
  event_date,
  platform,
  country,
  version,
  source,
  target,
  COUNT(*) AS count
FROM iap_pairs
WHERE event_date between '2025-09-04' and '2025-09-07'
GROUP BY
  event_date,
  platform,
  country,
  version,
  source,
  target;




-- chapter_play_mode
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.chapter_play_mode`
(event_date, platform, version, chapter, user_start, event_start, user_start_next_level, 
 user_win, event_win, user_revive, event_revive, user_ad_reward, num_ad_reward, 
 user_iap, num_iap, mode)
WITH start_chapter AS (
  SELECT 
    event_date, platform, version,
    CAST(SPLIT(chapter_wave, "_")[OFFSET(0)] AS STRING) AS chapter,
    COUNT(DISTINCT user_pseudo_id) AS user_start,
    COUNT(user_pseudo_id) AS event_start
  FROM `bounce-survivor---puzzle-rpg.flatten_table.start_chapter_wave`
  WHERE CAST(SPLIT(chapter_wave, "_")[OFFSET(1)] AS INT64) = 1
    AND event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
start_chapter_next AS (
  SELECT *,
         LEAD(user_start) OVER (PARTITION BY event_date, platform, version ORDER BY CAST(chapter AS INT64)) AS user_start_next_level
  FROM start_chapter
),
win_chapter AS (
  SELECT event_date, platform, version,
         CAST(SPLIT(chapter_wave, "_")[OFFSET(0)] AS STRING) AS chapter,
         COUNT(DISTINCT user_pseudo_id) AS user_win,
         COUNT(user_pseudo_id) AS event_win
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
revive_chapter AS (
  SELECT event_date, platform, version,
         CAST(chapter AS STRING) AS chapter,
         COUNT(DISTINCT user_pseudo_id) AS user_revive,
         COUNT(user_pseudo_id) AS event_revive
  FROM `bounce-survivor---puzzle-rpg.flatten_table.revive_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
reward_chapter AS (
  SELECT event_date, platform, version,
         CAST(SPLIT(chapter_wave, "_")[OFFSET(0)] AS STRING) AS chapter,
         COUNT(DISTINCT user_pseudo_id) AS user_ad_reward,
         COUNT(user_pseudo_id) AS num_ad_reward
  FROM `bounce-survivor---puzzle-rpg.flatten_table.af_rewarded`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
iap_chapter AS (
  SELECT event_date, platform, version,
         CAST(chapter AS STRING) AS chapter,
         COUNT(DISTINCT user_pseudo_id) AS user_iap,
         COUNT(user_pseudo_id) AS num_iap
  FROM `bounce-survivor---puzzle-rpg.flatten_table.iap_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
chapter_mode AS (
  SELECT 
    a.event_date, a.platform, a.version,
    a.chapter,
    a.user_start, a.event_start, a.user_start_next_level,
    w.user_win, w.event_win,
    r.user_revive, r.event_revive,
    ad.user_ad_reward, ad.num_ad_reward,
    i.user_iap, i.num_iap,
    'chapter' AS mode
  FROM start_chapter_next a
  LEFT JOIN win_chapter    w USING(event_date, platform, version, chapter)
  LEFT JOIN revive_chapter r USING(event_date, platform, version, chapter)
  LEFT JOIN reward_chapter ad USING(event_date, platform, version, chapter)
  LEFT JOIN iap_chapter    i USING(event_date, platform, version, chapter)
),
base_wave AS (
  SELECT event_date, platform, version,
         chapter_wave,
         COUNT(DISTINCT user_pseudo_id) AS user_start,
         COUNT(user_pseudo_id) AS event_start
  FROM `bounce-survivor---puzzle-rpg.flatten_table.start_chapter_wave`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
start_wave_next AS (
  SELECT *,
         LEAD(user_start) OVER (PARTITION BY event_date, platform, version ORDER BY CAST(CONCAT(SPLIT(chapter_wave, "_")[OFFSET(0)], ".", LPAD(SPLIT(chapter_wave, "_")[OFFSET(1)], 2, '0')) AS FLOAT64)) AS user_start_next_level
  FROM base_wave
),
win_wave AS (
  SELECT event_date, platform, version, chapter_wave,
         COUNT(DISTINCT user_pseudo_id) AS user_win,
         COUNT(user_pseudo_id) AS event_win
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
revive_wave AS (
  SELECT event_date, platform, version,
         CONCAT(CAST(chapter AS STRING), "_", CAST(wave AS STRING)) AS chapter_wave,
         COUNT(DISTINCT user_pseudo_id) AS user_revive,
         COUNT(user_pseudo_id) AS event_revive
  FROM `bounce-survivor---puzzle-rpg.flatten_table.revive_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
reward_wave AS (
  SELECT event_date, platform, version, chapter_wave,
         COUNT(DISTINCT user_pseudo_id) AS user_ad_reward,
         COUNT(user_pseudo_id) AS num_ad_reward
  FROM `bounce-survivor---puzzle-rpg.flatten_table.af_rewarded`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
iap_wave AS (
  SELECT event_date, platform, version, chapter_wave,
         COUNT(DISTINCT user_pseudo_id) AS user_iap,
         COUNT(user_pseudo_id) AS num_iap
  FROM `bounce-survivor---puzzle-rpg.flatten_table.iap_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4
),
chapter_wave_mode AS (
  SELECT 
    a.event_date, a.platform, a.version,
    CAST(CONCAT(SPLIT(a.chapter_wave, "_")[OFFSET(0)], ".", LPAD(SPLIT(a.chapter_wave, "_")[OFFSET(1)], 2, '0')) AS FLOAT64) AS chapter_wave,
    a.user_start, a.event_start, a.user_start_next_level,
    w.user_win, w.event_win,
    r.user_revive, r.event_revive,
    ad.user_ad_reward, ad.num_ad_reward,
    i.user_iap, i.num_iap,
    'chapter_wave' AS mode
  FROM start_wave_next a
  LEFT JOIN win_wave    w USING(event_date, platform, version, chapter_wave)
  LEFT JOIN revive_wave r USING(event_date, platform, version, chapter_wave)
  LEFT JOIN reward_wave ad USING(event_date, platform, version, chapter_wave)
  LEFT JOIN iap_wave    i USING(event_date, platform, version, chapter_wave)
)
SELECT event_date, platform, version, CAST(chapter AS FLOAT64) AS chapter, user_start, event_start, user_start_next_level, user_win,
event_win, user_revive, event_revive, user_ad_reward, num_ad_reward, user_iap, num_iap, mode 
FROM chapter_mode
UNION ALL
SELECT event_date, platform, version, CAST(chapter_wave AS FLOAT64) AS chapter, user_start, event_start, user_start_next_level, user_win,
event_win, user_revive, event_revive, user_ad_reward, num_ad_reward, user_iap, num_iap, mode 
FROM chapter_wave_mode;












-- hero_winrate
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.hero_winrate`
(event_date, platform, version, chapter, hero, start_event, start_user, win_event, win_user)
WITH start_general AS (
  SELECT 
    event_date, 
    platform, 
    version, 
    chapter,
    'general' AS hero,
    COUNT(user_pseudo_id) AS start_event,
    COUNT(DISTINCT user_pseudo_id) AS start_user
  FROM `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5
),
win_general AS (
  SELECT 
    event_date, 
    platform, 
    version, 
    CAST(SPLIT(chapter_wave, "_")[OFFSET(0)] AS INT64) AS chapter,
    'general' AS hero,
    COUNT(user_pseudo_id) AS win_event,
    COUNT(DISTINCT user_pseudo_id) AS win_user
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5
),
hero_start AS (
  SELECT 
    event_date, 
    platform, 
    version, 
    chapter,
    hero,
    COUNT(user_pseudo_id) AS start_event,
    COUNT(DISTINCT user_pseudo_id) AS start_user
  FROM `bounce-survivor---puzzle-rpg.flatten_table.join_chapter`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5
),
hero_win AS (
  SELECT 
    event_date, 
    platform, 
    version, 
    chapter,
    hero,
    COUNT(user_pseudo_id) AS win_event,
    COUNT(DISTINCT user_pseudo_id) AS win_user
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter_hero`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5
),
general_stats AS (
  SELECT 
    sg.event_date,
    sg.platform,
    sg.version,
    sg.chapter,
    sg.hero,
    sg.start_event,
    sg.start_user,
    wg.win_event,
    wg.win_user
  FROM start_general sg
  LEFT JOIN win_general wg
    USING (event_date, platform, version, chapter, hero)
),
hero_stats AS (
  SELECT 
    hs.event_date,
    hs.platform,
    hs.version,
    hs.chapter,
    hs.hero,
    hs.start_event,
    hs.start_user,
    hw.win_event, 
    hw.win_user
  FROM hero_start hs
  LEFT JOIN hero_win hw
    USING (event_date, platform, version, chapter, hero)
)
SELECT * 
FROM general_stats
UNION ALL
SELECT * 
FROM hero_stats 
WHERE hero IN ('Nick','Johnson','Apelido','Grambit','Cpt. Cappy','Mella','Jick','Lampo','Gothy','Luji');






-- daily_challenge 
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.daily_challenge`
WITH b AS ( 
  SELECT 
    event_date, 
    version, 
    platform, 
    COUNT(DISTINCT user_pseudo_id) AS user_start_daily_challenge  
  FROM `bounce-survivor---puzzle-rpg.flatten_table.scavenge_beyond_start`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3
), a AS (
  SELECT 
    event_date, 
    version, 
    platform,
    IFNULL(COUNT(DISTINCT user_pseudo_id),0) AS user_start_chapter
  FROM `bounce-survivor---puzzle-rpg.flatten_table.session_start`
  WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
    AND user_pseudo_id IN (
      SELECT DISTINCT user_pseudo_id 
      FROM `bounce-survivor---puzzle-rpg.flatten_table.unlock_scavenge_beyond_feature`
      WHERE event_date BETWEEN '2025-09-08' AND '2025-09-24'
    )
  GROUP BY 1,2,3
)
SELECT 
  a.event_date, 
  a.version, 
  a.platform, 
  a.user_start_chapter, 
  b.user_start_daily_challenge
FROM a 
LEFT JOIN b USING(event_date, version, platform);




-- day_sesion_play
INSERT INTO `bounce-survivor---puzzle-rpg.dashboard_table.day_sesion_play`
(event_date, platform, version, day_session, num_play, num_user_play, num_chapter_play, 
 num_win, num_user_win, num_chapter_win, type)
WITH first_open_dates AS (
  SELECT
    user_pseudo_id,
    MIN(event_date) AS first_open_date
  FROM `bounce-survivor---puzzle-rpg.flatten_table.first_open`
  GROUP BY user_pseudo_id
), st AS (
  SELECT 
    a.user_pseudo_id, a.event_date, a.platform, a.version, a.ga_session_number,
    DATE_DIFF(a.event_date, b.first_open_date, DAY) AS Day_since_fo,
    COUNT(a.chapter) AS num_event_play,
    COUNT(DISTINCT a.chapter) AS num_chapter_play
  FROM `bounce-survivor---puzzle-rpg.flatten_table.play_chapter` a
  JOIN first_open_dates b ON a.user_pseudo_id = b.user_pseudo_id
  WHERE a.event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5,6
), w AS (
  SELECT 
    a.user_pseudo_id, a.event_date, a.platform, a.version, a.ga_session_number,
    DATE_DIFF(a.event_date, b.first_open_date, DAY) AS Day_since_fo,
    COUNT(a.chapter_wave) AS num_event_win,
    COUNT(DISTINCT a.chapter_wave) AS num_chapter_win        
  FROM `bounce-survivor---puzzle-rpg.flatten_table.win_chapter` a
  JOIN first_open_dates b ON a.user_pseudo_id = b.user_pseudo_id
  WHERE a.event_date BETWEEN '2025-09-08' AND '2025-09-24'
  GROUP BY 1,2,3,4,5,6  
), cohort_day_start AS (
  SELECT 
    event_date, platform, version, st.Day_since_fo, 
    COUNT(DISTINCT user_pseudo_id) AS num_user_play,
    SUM(num_event_play) AS num_play,
    SUM(num_chapter_play) AS num_chapter_play
  FROM st 
  GROUP BY 1,2,3,4
), cohort_day_win AS (
  SELECT 
    event_date, platform, version, Day_since_fo, 
    COUNT(DISTINCT user_pseudo_id) AS num_user_win,
    SUM(num_event_win) AS num_win,
    SUM(num_chapter_win) AS num_chapter_win
  FROM w 
  GROUP BY 1,2,3,4  
), session_number_start AS (
  SELECT 
    event_date, platform, version, ga_session_number, 
    COUNT(DISTINCT user_pseudo_id) AS num_user_play,
    SUM(num_event_play) AS num_play,
    SUM(num_chapter_play) AS num_chapter_play
  FROM st 
  GROUP BY 1,2,3,4
), session_number_win AS (
  SELECT 
    event_date, platform, version, ga_session_number, 
    COUNT(DISTINCT user_pseudo_id) AS num_user_win,
    SUM(num_event_win) AS num_win,
    SUM(num_chapter_win) AS num_chapter_win
  FROM w 
  GROUP BY 1,2,3,4  
)
SELECT 
  a.event_date, a.platform, a.version, a.Day_since_fo AS day_session, 
  a.num_play, a.num_user_play, a.num_chapter_play, 
  b.num_win, b.num_user_win, b.num_chapter_win,
  'cohort_day' AS type 
FROM cohort_day_start a
LEFT JOIN cohort_day_win b 
  USING (event_date, platform, version, Day_since_fo)

UNION ALL

SELECT 
  a.event_date, a.platform, a.version, a.ga_session_number AS day_session, 
  a.num_play, a.num_user_play, a.num_chapter_play, 
  b.num_win, b.num_user_win, b.num_chapter_win,
  'session_number' AS type 
FROM session_number_start a 
LEFT JOIN session_number_win b 
  USING (event_date, platform, version, ga_session_number);










