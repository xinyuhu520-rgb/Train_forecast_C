-- ============================================================================
-- 火车票日期维度表 2025-2027（临时表）
-- 表名：tmp_bu_trn.dim_trn_date_label_2025_2027_df
-- 分区：d='2026-10-14'
--
-- 口径说明：
--   1. 2025、2026：纯日历及历史假期原样读取自 dim_traindb.dim_trn_date_label_df
--   2. 2026-12-18 ~ 2026-12-31：补齐 2027 元旦开抢第 1 ~ 14 天（线上该段尚未填，holiday_name 为空）
--   3. 2027 全年：按国务院 2027 正式放假通知生成 7 个假期（元旦3天、春节9天[2.5-2.13]、清明3天、劳动5天、端午1天[6.9]、中秋1天[9.15]、国庆7天）
--   4. 开抢期规则：放假前 14 天到放假结束（开抢天数 = N + 14）
--   5. 2027-12-18 ~ 2027-12-31：填入 2028 元旦开抢第 1 ~ 14 天
--   6. is_tiaoxiu_workday：官方调休上班日（2027-01-31, 2027-02-14, 2027-05-08, 2027-09-26, 2027-10-09）
--
-- 执行引擎：adhoc HIVE 引擎（exec-mode=HIVE）
-- ============================================================================

-- ---------- 1. 建表（若不存在） ----------
CREATE TABLE IF NOT EXISTS tmp_bu_trn.dim_trn_date_label_2025_2027_df (
  `date_solar`          string COMMENT '阳历日期，作为主键',
  `date_lunar`          string COMMENT '农历日期',
  `day_of_week`         int    COMMENT '数字表示星期几(1-7)',
  `day_of_month`        int    COMMENT '当前日期在当月中的天数',
  `day_of_year`         int    COMMENT '当前日期在当年中的天数',
  `week_of_year`        int    COMMENT '当前日期在当年中的周数',
  `firstday_of_week`    string COMMENT '所在周的第一天日期',
  `lastday_of_week`     string COMMENT '所在周的最后一天日期',
  `is_weekend`          int    COMMENT '是否为周末(1:是, 0:否)',
  `holiday_name`        string COMMENT '主要节假日名称(如:国庆假期)',
  `holiday_day_name`    string COMMENT '假期出行当天名称(如:国庆出行第1天)',
  `holiday_day_index`   int    COMMENT '假期出行天数序号(从第1天开始)',
  `booking_day_name`    string COMMENT '假期开抢当天名称(如:国庆开抢第1天)',
  `booking_day_index`   int    COMMENT '假期开抢天数序号(从提前14天开始)',
  `days_to_cny_eve`     int    COMMENT '距离当年或次年除夕天数',
  `is_tiaoxiu_workday`  int    COMMENT '调休加班日标记(1:调休上班, 0:否)'
)
COMMENT '2025-2027时间维度临时表，2027按规则生成待微调'
PARTITIONED BY (`d` string COMMENT 'date分区键')
STORED AS ORC
;

-- ---------- 2. 灌数（覆盖写入指定分区） ----------
INSERT OVERWRITE TABLE tmp_bu_trn.dim_trn_date_label_2025_2027_df PARTITION (d='2026-10-14')
SELECT
  date_solar, date_lunar, day_of_week, day_of_month, day_of_year,
  week_of_year, firstday_of_week, lastday_of_week, is_weekend,

  -- holiday_name: 线上有值优先；线上无值时，仅放假区间内填充
  COALESCE(CASE WHEN holiday_name = '' THEN NULL ELSE holiday_name END,
           CASE WHEN hol_grp IS NOT NULL THEN concat(hol_grp, '假期') END)
    AS holiday_name,

  -- holiday_day_name
  COALESCE(CASE WHEN holiday_day_name = '' THEN NULL ELSE holiday_day_name END,
           CASE WHEN hol_grp IS NOT NULL
                THEN concat(hol_grp, '出行第', CAST(datediff(date_solar, hol_st) + 1 AS string), '天') END)
    AS holiday_day_name,

  -- holiday_day_index
  COALESCE(holiday_day_index,
           CASE WHEN hol_grp IS NOT NULL THEN datediff(date_solar, hol_st) + 1 END)
    AS holiday_day_index,

  -- booking_day_name: 2026-12-18~2027-01-03 连续生成元旦开抢第1~17天
  COALESCE(CASE WHEN booking_day_name = '' THEN NULL ELSE booking_day_name END,
           CASE WHEN bk_grp IS NOT NULL
                THEN concat(bk_grp, '开抢第', CAST(datediff(date_solar, bk_st) + 1 AS string), '天') END)
    AS booking_day_name,

  -- booking_day_index
  COALESCE(booking_day_index,
           CASE WHEN bk_grp IS NOT NULL THEN datediff(date_solar, bk_st) + 1 END)
    AS booking_day_index,

  days_to_cny_eve,

  -- 调休加班日标记 (国务院官方 2027 调休上班日)
  CASE WHEN date_solar IN ('2027-01-31', '2027-02-14', '2027-05-08', '2027-09-26', '2027-10-09') THEN 1 ELSE 0 END AS is_tiaoxiu_workday
FROM (
  SELECT
    date_solar, date_lunar, day_of_week, day_of_month, day_of_year,
    week_of_year, firstday_of_week, lastday_of_week, is_weekend,
    holiday_name, holiday_day_name, holiday_day_index,
    booking_day_name, booking_day_index, days_to_cny_eve,

    -- 2027 国务院正式放假区间 (仅在此区间内才是假期)
    CASE
      WHEN date_solar BETWEEN '2027-01-01' AND '2027-01-03' THEN '元旦'
      WHEN date_solar BETWEEN '2027-02-05' AND '2027-02-13' THEN '春节'
      WHEN date_solar BETWEEN '2027-04-03' AND '2027-04-05' THEN '清明'
      WHEN date_solar BETWEEN '2027-05-01' AND '2027-05-05' THEN '劳动'
      WHEN date_solar BETWEEN '2027-06-09' AND '2027-06-09' THEN '端午'
      WHEN date_solar BETWEEN '2027-09-15' AND '2027-09-15' THEN '中秋'
      WHEN date_solar BETWEEN '2027-10-01' AND '2027-10-07' THEN '国庆'
    END AS hol_grp,
    CASE
      WHEN date_solar BETWEEN '2027-01-01' AND '2027-01-03' THEN '2027-01-01'
      WHEN date_solar BETWEEN '2027-02-05' AND '2027-02-13' THEN '2027-02-05'
      WHEN date_solar BETWEEN '2027-04-03' AND '2027-04-05' THEN '2027-04-03'
      WHEN date_solar BETWEEN '2027-05-01' AND '2027-05-05' THEN '2027-05-01'
      WHEN date_solar BETWEEN '2027-06-09' AND '2027-06-09' THEN '2027-06-09'
      WHEN date_solar BETWEEN '2027-09-15' AND '2027-09-15' THEN '2027-09-15'
      WHEN date_solar BETWEEN '2027-10-01' AND '2027-10-07' THEN '2027-10-01'
    END AS hol_st,

    -- 开抢期 (提前14天到放假结束)
    CASE
      WHEN date_solar BETWEEN '2026-12-18' AND '2027-01-03' THEN '元旦'
      WHEN date_solar BETWEEN '2027-01-22' AND '2027-02-13' THEN '春节'
      WHEN date_solar BETWEEN '2027-03-20' AND '2027-04-05' THEN '清明'
      WHEN date_solar BETWEEN '2027-04-17' AND '2027-05-05' THEN '劳动'
      WHEN date_solar BETWEEN '2027-05-26' AND '2027-06-09' THEN '端午'
      WHEN date_solar BETWEEN '2027-09-01' AND '2027-09-15' THEN '中秋'
      WHEN date_solar BETWEEN '2027-09-17' AND '2027-10-07' THEN '国庆'
      WHEN date_solar BETWEEN '2027-12-18' AND '2027-12-31' THEN '元旦'
    END AS bk_grp,
    CASE
      WHEN date_solar BETWEEN '2026-12-18' AND '2027-01-03' THEN '2026-12-18'
      WHEN date_solar BETWEEN '2027-01-22' AND '2027-02-13' THEN '2027-01-22'
      WHEN date_solar BETWEEN '2027-03-20' AND '2027-04-05' THEN '2027-03-20'
      WHEN date_solar BETWEEN '2027-04-17' AND '2027-05-05' THEN '2027-04-17'
      WHEN date_solar BETWEEN '2027-05-26' AND '2027-06-09' THEN '2027-05-26'
      WHEN date_solar BETWEEN '2027-09-01' AND '2027-09-15' THEN '2027-09-01'
      WHEN date_solar BETWEEN '2027-09-17' AND '2027-10-07' THEN '2027-09-17'
      WHEN date_solar BETWEEN '2027-12-18' AND '2027-12-31' THEN '2027-12-18'
    END AS bk_st
  FROM dim_traindb.dim_trn_date_label_df
  WHERE d = '2026-10-14'
    AND date_solar BETWEEN '2025-01-01' AND '2027-12-31'
) t
;
