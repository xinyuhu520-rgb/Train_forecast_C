-- ================================================================
-- 名称：火车票2027实绩与预测一体化落表 (adm-trn-ord-forecast-2027-combined-daily)
-- 功能描述：将 2026 实际业务数据与 2027 三档预测数据做全外连接融合落表。
-- 已做三档历史基准对齐，无论筛选中性、保守、悲观均具备 2026-2027 完整连续无断点曲线。
-- 输入表：
--   mplat_bi_traindb.adm_trn_forecast_2027_daily_df (上游 Python 预测结果表)
--   dms_traindb.cdm_dms_trn_ord_core_idx_sum_di (业务核心指标表，严格 Ctrip.Train 纯盘)
-- 输出表：
--   mplat_bi_traindb.adm_trn_ord_forecast_2027_combined_di (实绩与预测一体化表)
-- 创建人：hxy胡馨予
-- 创建时间：2026-10-19
-- 运行类型：Hive SQL / Spark SQL
-- 执行账号：trn_mplat_bi
-- 调度依赖：挂载依赖上游任务 1618901，完成后自动唤醒
-- ================================================================

INSERT OVERWRITE TABLE mplat_bi_traindb.adm_trn_ord_forecast_2027_combined_di PARTITION (d = '${zdt.format("yyyy-MM-dd")}')
SELECT
  '${zdt.format("yyyy-MM-dd")}' AS forecast_date,
  coalesce(p.target_date, a.target_date) AS target_date,
  substr(coalesce(p.target_date, a.target_date), 1, 4) AS target_year,
  substr(coalesce(p.target_date, a.target_date), 1, 7) AS target_month,
  coalesce(p.tier, a.tier) AS tier,
  coalesce(p.line, a.line) AS line,
  CASE
    WHEN a.act_issued_cnt IS NOT NULL AND p.pred_issued_cnt IS NOT NULL THEN '实际与预测重叠期'
    WHEN a.act_issued_cnt IS NOT NULL THEN '历史实际期'
    ELSE '未来预测期'
  END AS period_type,

  -- 统一拼接度量 (全时段 730 天 100% 无 NULL，万张/万元)
  round(coalesce(a.act_issued_cnt, p.pred_issued_cnt) / 1e4, 2) AS combined_issued_w,
  round(coalesce(a.act_net_cnt, p.pred_net_cnt) / 1e4, 2) AS combined_net_w,
  round(coalesce(a.act_profit_amt, p.pred_profit_amt) / 1e4, 2) AS combined_profit_w,

  -- 独立实际值度量 (仅历史上存在真实值，未来为 NULL)
  round(a.act_issued_cnt / 1e4, 2) AS act_issued_w,
  round(a.act_net_cnt / 1e4, 2) AS act_net_w,
  round(a.act_profit_amt / 1e4, 2) AS act_profit_w,
  round(a.act_refund_cnt / a.act_issued_cnt * 100, 2) AS act_refund_rate_pct,
  round(a.act_profit_amt / a.act_net_cnt, 2) AS act_unit_profit,

  -- 独立预测值度量 (仅未来和重叠期有值，历史上为 NULL)
  round(p.pred_issued_cnt / 1e4, 2) AS pred_issued_w,
  round(p.pred_net_cnt / 1e4, 2) AS pred_net_w,
  round(p.pred_profit_amt / 1e4, 2) AS pred_profit_w,
  round(p.pred_refund_cnt / p.pred_issued_cnt * 100, 2) AS pred_refund_rate_pct,
  round(p.pred_profit_amt / p.pred_net_cnt, 2) AS pred_unit_profit,

  -- 偏差率追踪 (仅重叠期有值)
  round((p.pred_issued_cnt - a.act_issued_cnt) / a.act_issued_cnt * 100, 2) AS issued_diff_pct,
  round((p.pred_net_cnt - a.act_net_cnt) / a.act_net_cnt * 100, 2) AS net_diff_pct
FROM (
  -- 1. 预测数据：读取上游任务 1618901 产出的最新预测大盘
  SELECT
    target_date, line, '预测' AS data_source, tier,
    pred_issued_cnt, pred_net_cnt, pred_refund_cnt, pred_profit AS pred_profit_amt
  FROM mplat_bi_traindb.adm_trn_forecast_2027_daily_df
  WHERE d = coalesce(
    nullif('${zdt.format("yyyy-MM-dd")}', ''),
    (SELECT d FROM mplat_bi_traindb.adm_trn_forecast_2027_daily_df ORDER BY d DESC LIMIT 1)
  )
) p
FULL OUTER JOIN (
  -- 2. 真实业务数据：对齐三档，保证无论筛选哪个档位，历史 2026 全年均有完整实际基准
  SELECT
    act_raw.business_date AS target_date,
    act_raw.line,
    '实际' AS data_source,
    t.tier,
    act_raw.act_issued_cnt,
    act_raw.act_net_cnt,
    act_raw.act_refund_cnt,
    act_raw.act_profit_amt
  FROM (
    SELECT
      b.business_date,
      b.solution_business_type_cn AS line,
      sum(b.agent_ticket_cnt) AS act_issued_cnt,
      sum(b.net_agent_ticket_cnt) AS act_net_cnt,
      sum(b.refund_ticket_cnt) AS act_refund_cnt,
      sum(b.profit_amt) AS act_profit_amt
    FROM dms_traindb.cdm_dms_trn_ord_core_idx_sum_di b
    WHERE b.d >= '2026-01-01'
      AND b.partner_name = 'Ctrip.Train'
      AND b.solution_business_type_cn IN ('纯买', '买得到', '纯抢')
    GROUP BY b.business_date, b.solution_business_type_cn
    UNION ALL
    SELECT
      b.business_date,
      '合计' AS line,
      sum(b.agent_ticket_cnt) AS act_issued_cnt,
      sum(b.net_agent_ticket_cnt) AS act_net_cnt,
      sum(b.refund_ticket_cnt) AS act_refund_cnt,
      sum(b.profit_amt) AS act_profit_amt
    FROM dms_traindb.cdm_dms_trn_ord_core_idx_sum_di b
    WHERE b.d >= '2026-01-01'
      AND b.partner_name = 'Ctrip.Train'
      AND b.solution_business_type_cn IN ('纯买', '买得到', '纯抢')
    GROUP BY b.business_date
  ) act_raw
  CROSS JOIN (
    SELECT '中性' AS tier UNION ALL SELECT '保守' UNION ALL SELECT '悲观'
  ) t
) a
  ON p.target_date = a.target_date AND p.line = a.line AND p.tier = a.tier;
