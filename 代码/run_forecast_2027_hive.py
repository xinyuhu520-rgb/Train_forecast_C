# -*- coding: utf-8 -*-
"""
火车票 2027 业务预测主执行脚本 (生产落表版)

业务背景与设定：
  1. 彻底剔除 2024 年数据，仅基于 2025 全年 + 2026 实际数据训练
  2. 范围：Ctrip.Train (C端)，纯买 / 买得到(智慧) / 纯抢 3条产线
  3. 口径：出票日/flow口径，净票量 = 出票 - 退票，收益 = 净票量 * 单票收益
  4. 产线分化假设（用户拍板）：纯抢进一步萎缩，买得到/纯买份额微升
       - 中性: 纯买 -9%, 买得到 -7%, 纯抢 -22% (总盘 -9.8%)
       - 保守: 纯买 -12%, 买得到 -10%, 纯抢 -26% (总盘 -13.0%)
       - 悲观: 纯买 -15%, 买得到 -13%, 纯抢 -32% (总盘 -16.3%)
  5. 监管约束下的单收预期（用户拍板，仅限 C 端 Ctrip.Train 口径）：
       - 日常基准取 2026 Q2~Q4 新常态：纯买 2.91 元, 买得到 3.56 元, 纯抢 8.03 元
       - 2027 春节变现溢价受限：纯抢 1.35x (~10.84元, 显著低于2026年13.6元), 买得到 1.40x (~5.00元), 纯买 1.08x (~3.13元)
       - 其他小长假溢价：纯抢 1.15x, 买得到 1.10x, 纯买 1.03x
  6. 落地形式：写入 Hive 表 tmp_bu_trn.trn_forecast_2027_daily_df PARTITION (d='YYYY-MM-DD')
  7. 产出规模：443 天（2026-10-15 至 2027-12-31）× 3 档位 × 4 产线 = 5,316 行，全量幂等覆盖当天分区

执行方式：python3 run_forecast_2027_hive.py [--date YYYY-MM-DD]
依赖环境：Python 3.10+, pandas, numpy, scikit-learn, adhoc_sql.py (Chrome 登录态)
"""
import argparse
import json
import os
import subprocess
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor

SCRIPTS = '/Users/xinyhu/.agents/skills/adhoc-artnova-data-access/scripts'
TARGET_TABLE = 'mplat_bi_traindb.adm_trn_forecast_2027_daily_df'
DIM_TABLE = 'dim_traindb.dim_trn_date_label_df'

LINES = ['纯买', '买得到', '纯抢']
WIN = 28
ALL_HOLS = ['元旦', '春节', '清明', '劳动', '端午', '中秋', '国庆']

MAKEUP_WORKDAYS_2027 = ['2027-01-31', '2027-02-14', '2027-05-08', '2027-09-26', '2027-10-09']
SINGLE_DAY_PULSE_KEEP = {'纯买': 0.45, '买得到': 0.30, '纯抢': 0.25}

# 三档相对基准的动态偏移量（核心收缩率由模型从数仓最新 2025 vs 2026 实绩动态自学习并随时间与供需演进）
TIER_OFFSETS = {
    '中性': {'纯买': 0.00, '买得到': 0.00, '纯抢': 0.00},
    '保守': {'纯买': -0.03, '买得到': -0.035, '纯抢': -0.05},
    '悲观': {'纯买': -0.06, '买得到': -0.07, '纯抢': -0.10},
}
TIME_DRIFT_MAP = {'纯买': +0.05, '买得到': -0.02, '纯抢': -0.10}

# 2026 下半年日常单收新常态基准 (仅 C 端 Ctrip.Train)
UNIT_PROFIT_BASE = {
    '纯买': 2.91,
    '买得到': 3.56,
    '纯抢': 8.03,
}

# 变现溢价系数（体现持续监管下的溢价压制）
PREMIUM_CNY = {'纯买': 1.08, '买得到': 1.40, '纯抢': 1.35}
PREMIUM_HOL = {'纯买': 1.03, '买得到': 1.10, '纯抢': 1.15}


def normalize_dim_calendar(df):
    """直接信任并使用中台稳定维表 dim_traindb.dim_trn_date_label_df 的节假日与开抢期标签，仅做空值规范化与调休上班日标记"""
    out = df.copy()
    out['date_str'] = out['dt'].dt.strftime('%Y-%m-%d')
    out['hol_name'] = out['hol_name'].replace({'null': '', None: ''}).fillna('')
    out['bk_name'] = out['bk_name'].replace({'null': '', None: ''}).fillna('')
    out['hol_idx'] = pd.to_numeric(out['hol_idx'], errors='coerce').fillna(0)
    out['bk_idx'] = pd.to_numeric(out['bk_idx'], errors='coerce').fillna(0)

    out.loc[out['date_str'].isin(MAKEUP_WORKDAYS_2027), 'is_wknd'] = 0

    m_ny28 = (out['date_str'] >= '2027-12-18') & (out['date_str'] <= '2027-12-31') & (out['bk_name'] == '')
    if m_ny28.any():
        out.loc[m_ny28, 'bk_name'] = '元旦开抢第' + ((out.loc[m_ny28, 'dt'] - pd.to_datetime('2027-12-18')).dt.days + 1).astype(str) + '天'
        out.loc[m_ny28, 'bk_idx'] = (out.loc[m_ny28, 'dt'] - pd.to_datetime('2027-12-18')).dt.days + 1

    return out


def run_adhoc_sql(sql, label="SQL", exec_mode="STARROCKS_HIVE", timeout=600):
    r = subprocess.run(
        ['python3', 'adhoc_sql.py', '--browser', 'chrome', '--exec-mode', exec_mode,
         '--insecure', '--limit-disable', '--format', 'json', '--sql', sql],
        cwd=SCRIPTS, capture_output=True, text=True, timeout=timeout
    )
    txt = r.stdout
    k = txt.find('{')
    if k < 0:
        raise RuntimeError(f"[{label}] 执行失败:\nstdout={txt[:500]}\nstderr={r.stderr[:500]}")
    d = json.loads(txt[k:txt.rindex('}') + 1])
    if d.get('status') != 'FINISHED':
        raise RuntimeError(f"[{label}] 异常未完成: {str(d)[:500]}")
    return d


def load_history_and_dim(forecast_date):
    """加载 2025 至 forecast_date 前一天的历史业务数据，以及中台稳定维表 dim_traindb.dim_trn_date_label_df"""
    yesterday = (pd.to_datetime(forecast_date) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    res_part = run_adhoc_sql(f"SELECT d FROM {DIM_TABLE} GROUP BY d ORDER BY d DESC LIMIT 1", "获取时间维表最新分区")
    dim_part = res_part['rows'][0][0]
    print(f"1. 从数仓加载业务数据（2025-01-01 至 {yesterday}）与中台时间维表 {DIM_TABLE} (d='{dim_part}')...")
    
    # 历史业务数据 + 维度
    sql_hist = f"""
    SELECT
      b.business_date,
      b.solution_business_type_cn AS line,
      sum(b.agent_ticket_cnt) AS issued_cnt,
      sum(b.refund_ticket_cnt) AS refund_cnt,
      sum(b.net_agent_ticket_cnt) AS net_cnt,
      sum(b.profit_amt) AS profit,
      d.day_of_week,
      d.is_weekend,
      d.holiday_name,
      d.holiday_day_name,
      d.holiday_day_index,
      d.booking_day_name,
      d.booking_day_index,
      d.days_to_cny_eve
    FROM dms_traindb.cdm_dms_trn_ord_core_idx_sum_di b
    INNER JOIN {DIM_TABLE} d
      ON b.business_date = d.date_solar
    WHERE b.d BETWEEN '2025-01-01' AND '{yesterday}'
      AND d.d = '{dim_part}'
      AND b.partner_name = 'Ctrip.Train'
      AND b.solution_business_type_cn IN ('纯买', '买得到', '纯抢')
    GROUP BY
      b.business_date,
      b.solution_business_type_cn,
      d.day_of_week,
      d.is_weekend,
      d.holiday_name,
      d.holiday_day_name,
      d.holiday_day_index,
      d.booking_day_name,
      d.booking_day_index,
      d.days_to_cny_eve
    ORDER BY b.business_date, b.solution_business_type_cn
    """
    res_hist = run_adhoc_sql(sql_hist, "拉取历史数据")
    cols_h = ['date', 'line', 'issued', 'refund', 'net', 'profit', 'dow', 'is_wknd',
              'hol_name', 'hol_day_name', 'hol_idx', 'bk_name', 'bk_idx', 'd2cny']
    df_h = pd.DataFrame(res_hist['rows'], columns=cols_h).replace({'null': None, '': None})
    for c in ['issued', 'refund', 'net', 'profit']: df_h[c] = df_h[c].astype(float)
    for c in ['dow', 'is_wknd', 'hol_idx', 'bk_idx', 'd2cny']: df_h[c] = pd.to_numeric(df_h[c], errors='coerce')
    df_h['dt'] = pd.to_datetime(df_h['date'])
    df_h['refund_rate'] = df_h['refund'] / df_h['issued']

    # 未来预测区间维度数据
    print(f"2. 从维度表加载未来预测日历（2026-10-15 至 2027-12-31）...")
    sql_fut = f"""
    SELECT
      date_solar, day_of_week, is_weekend, holiday_name, holiday_day_name,
      holiday_day_index, booking_day_name, booking_day_index, days_to_cny_eve
    FROM {DIM_TABLE}
    WHERE d = '{dim_part}'
      AND date_solar BETWEEN '2026-10-15' AND '2027-12-31'
    ORDER BY date_solar
    """
    res_fut = run_adhoc_sql(sql_fut, "拉取未来维度数据")
    cols_f = ['date', 'dow', 'is_wknd', 'hol_name', 'hol_day_name', 'hol_idx', 'bk_name', 'bk_idx', 'd2cny']
    df_f = pd.DataFrame(res_fut['rows'], columns=cols_f).replace({'null': None, '': None})
    for c in ['dow', 'is_wknd', 'hol_idx', 'bk_idx', 'd2cny']: df_f[c] = pd.to_numeric(df_f[c], errors='coerce')
    df_f['dt'] = pd.to_datetime(df_f['date'])

    df_h = normalize_dim_calendar(df_h)
    df_f = normalize_dim_calendar(df_f)
    return df_h, df_f


def featurize(df):
    """特征工程"""
    out = df.copy()
    out['month'] = out['dt'].dt.month
    out['woy'] = out['dt'].apply(lambda d: int(d.isocalendar()[1]))
    out['doy'] = out['dt'].dt.dayofyear
    out['sin_doy'] = np.sin(2.0 * np.pi * out['doy'] / 365.25)
    out['cos_doy'] = np.cos(2.0 * np.pi * out['doy'] / 365.25)
    out['sin_dow'] = np.sin(2.0 * np.pi * out['dow'] / 7.0)
    out['cos_dow'] = np.cos(2.0 * np.pi * out['dow'] / 7.0)
    out['bk_hol'] = out['bk_name'].str.extract(r'^(.+?)开抢')[0].fillna('无').replace({'国庆中秋': '国庆'})
    out['hol_grp'] = out['hol_name'].str.replace('假期', '', regex=False).fillna('无').replace({'国庆中秋': '国庆'})
    out['bk_idx_f'] = out['bk_idx'].fillna(0)
    out['hol_idx_f'] = out['hol_idx'].fillna(0)
    for h in ALL_HOLS:
        out[f'is_bk_{h}'] = (out['bk_hol'] == h).astype(int)
        out[f'is_hol_{h}'] = (out['hol_grp'] == h).astype(int)
    return out


def train_models(df_h):
    """训练 3 条产线的净票 Ratio GBDT、独立出票 Ratio GBDT、供需级联退票率 GBDT 与连续单收 GBDT 模型"""
    print("3. 训练产线级 V4.0 双轨 GBDT 模型（35天非假日中位数基线 + 180天半衰期样本衰减权重）...")
    lines_res = []
    for line in LINES:
        sub = df_h[df_h['line'] == line].sort_values('dt').copy()
        is_norm = (sub['bk_idx'].fillna(0) == 0) & (sub['hol_idx'].fillna(0) == 0) & (sub['d2cny'].abs() > 20)

        net_norm_only = sub['net'].copy()
        net_norm_only[~is_norm] = np.nan
        sub['base'] = net_norm_only.rolling(35, min_periods=5).median().shift(1).bfill().ffill()
        sub['ratio'] = sub['net'] / np.maximum(sub['base'], 1.0)

        iss_norm_only = sub['issued'].copy()
        iss_norm_only[~is_norm] = np.nan
        sub['iss_base'] = iss_norm_only.rolling(35, min_periods=5).median().shift(1).bfill().ffill()
        sub['iss_ratio'] = sub['issued'] / np.maximum(sub['iss_base'], 1.0)

        sub['unit_profit'] = sub['profit'] / np.maximum(sub['net'], 1.0)
        up_norm_only = sub['unit_profit'].copy()
        up_norm_only[~is_norm] = np.nan
        sub['up_base'] = up_norm_only.rolling(35, min_periods=5).median().shift(1).bfill().ffill()
        sub['up_ratio'] = np.clip(sub['unit_profit'] / np.maximum(sub['up_base'], 0.1), 0.65, 1.85)
        lines_res.append(sub)

    train_df = featurize(pd.concat(lines_res).sort_values(['dt', 'line']).reset_index(drop=True))
    train_df['year'] = train_df['dt'].dt.year
    train_df['is_peak'] = ((train_df['bk_idx_f'] > 0) | (train_df['hol_idx_f'] > 0) | (train_df['d2cny'].abs() <= 20)).astype(int)

    feats = ['dow', 'is_wknd', 'month', 'woy', 'doy', 'd2cny', 'bk_idx_f', 'hol_idx_f'] + \
            [f'is_bk_{h}' for h in ALL_HOLS] + [f'is_hol_{h}' for h in ALL_HOLS]
    rf_feats = feats + ['pred_ratio_feat', 'pred_iss_ratio_feat']
    up_feats = ['dow', 'is_wknd', 'month', 'sin_doy', 'cos_doy', 'sin_dow', 'cos_dow',
                'd2cny', 'bk_idx_f', 'hol_idx_f'] + \
               [f'is_bk_{h}' for h in ALL_HOLS] + [f'is_hol_{h}' for h in ALL_HOLS] + \
               ['pred_ratio_feat', 'pred_rf_feat']

    max_dt = train_df['dt'].max()
    ratio_models, iss_models, refund_models, up_models = {}, {}, {}, {}
    for line in LINES:
        sub = train_df[train_df['line'] == line].sort_values('dt').copy()
        sample_w = 0.5 ** ((max_dt - sub['dt']).dt.days.values / 180.0)

        m_r = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_r.fit(sub[feats], sub['ratio'], sample_weight=sample_w)
        ratio_models[line] = m_r

        m_iss = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_iss.fit(sub[feats], sub['iss_ratio'], sample_weight=sample_w)
        iss_models[line] = m_iss

        sub['pred_ratio_feat'] = m_r.predict(sub[feats])
        sub['pred_iss_ratio_feat'] = m_iss.predict(sub[feats])

        sub_rf = sub.dropna(subset=['refund_rate']).copy()
        w_rf = 0.5 ** ((max_dt - sub_rf['dt']).dt.days.values / 180.0)
        m_rf = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_rf.fit(sub_rf[rf_feats], sub_rf['refund_rate'], sample_weight=w_rf)
        refund_models[line] = m_rf

        sub['pred_rf_feat'] = m_rf.predict(sub[rf_feats])
        sub_up = sub.dropna(subset=['up_ratio']).copy()
        w_up = 0.5 ** ((max_dt - sub_up['dt']).dt.days.values / 180.0)
        m_up = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, min_samples_leaf=4, random_state=42)
        m_up.fit(sub_up[up_feats], sub_up['up_ratio'], sample_weight=w_up)
        up_models[line] = m_up

    return ratio_models, iss_models, refund_models, up_models, feats, rf_feats, up_feats, train_df


def predict_line_tier(line, tier, df_fut_line, m_ratio, m_iss, m_rf, m_up,
                      feats, rf_feats, up_feats, level_recent, level_iss_recent,
                      rf_scale_recent, up_level_recent, base_2026, iss_base_2026, dyn_param):
    """对单条产线单档位进行 V4.0 双轨解耦 + 三维动态 TIERS + 单日假脉冲衰减预测"""
    df = df_fut_line.copy()
    pred_ratio = m_ratio.predict(df[feats]).copy()
    pred_iss_ratio = m_iss.predict(df[feats]).copy()

    # 2027 官方节假日脉冲修正：从中台维表自动识别放假天数为1天的单日假（如端午、中秋），按产线衰减中长途与抢票超额脉冲
    hol_27_max = df[(df['dt'].dt.year == 2027) & (df['hol_grp'] != '无')].groupby('hol_grp')['hol_idx_f'].max()
    single_day_hols_2027 = list(hol_27_max[hol_27_max == 1].index)
    m_1d_hol = (df['dt'].dt.year == 2027) & (
        df['bk_hol'].isin(single_day_hols_2027) | df['hol_grp'].isin(single_day_hols_2027)
    )
    keep_k = SINGLE_DAY_PULSE_KEEP[line]
    m_r_pos = m_1d_hol & (pred_ratio > 1.0)
    pred_ratio[m_r_pos] = 1.0 + keep_k * (pred_ratio[m_r_pos] - 1.0)
    m_i_pos = m_1d_hol & (pred_iss_ratio > 1.0)
    pred_iss_ratio[m_i_pos] = 1.0 + keep_k * (pred_iss_ratio[m_i_pos] - 1.0)

    df['pred_ratio_feat'] = pred_ratio
    df['pred_iss_ratio_feat'] = pred_iss_ratio
    raw_pred_rf = m_rf.predict(df[rf_feats])

    # 1. 三维动态 TIERS 与水位 Level(t) 构造
    r_span = max(pred_ratio.max() - pred_ratio.min(), 1e-4)
    rf_span = max(raw_pred_rf.max() - raw_pred_rf.min(), 1e-4)
    tau = np.clip(0.5 * (pred_ratio - pred_ratio.min()) / r_span + 0.5 * (raw_pred_rf - raw_pred_rf.min()) / rf_span, 0.0, 1.0)

    n = len(df)
    prog = np.linspace(0.0, 1.0, n)
    r_norm_t = dyn_param['yoy_norm'] + prog * dyn_param['time_drift'] + TIER_OFFSETS[tier][line]
    r_peak_t = dyn_param['yoy_peak'] + 0.5 * prog * dyn_param['time_drift'] + TIER_OFFSETS[tier][line]
    dynamic_tier_rates = (1.0 - tau) * r_norm_t + tau * r_peak_t

    levels = np.zeros(n)
    iss_levels = np.zeros(n)
    rf_scales = np.zeros(n)
    level_types = []

    for i in range(n):
        level_tier_i = base_2026 * (1.0 + dynamic_tier_rates[i])
        iss_level_tier_i = iss_base_2026 * (1.0 + dynamic_tier_rates[i])
        if i <= 60:
            levels[i] = level_recent
            iss_levels[i] = level_iss_recent
            level_types.append('rolling_28d')
        elif i <= 180:
            alpha = (i - 60) / 120.0
            levels[i] = (1.0 - alpha) * level_recent + alpha * level_tier_i
            iss_levels[i] = (1.0 - alpha) * level_iss_recent + alpha * iss_level_tier_i
            level_types.append('transition')
        else:
            levels[i] = level_tier_i
            iss_levels[i] = iss_level_tier_i
            level_types.append('tier_target')

        if i <= 30:
            rf_scales[i] = rf_scale_recent
        elif i <= 90:
            rf_scales[i] = (1.0 - (i - 30) / 60.0) * rf_scale_recent + ((i - 30) / 60.0) * 1.0
        else:
            rf_scales[i] = 1.0

    # 2. 净票量与双轨融合出票量
    pred_net = levels * pred_ratio
    pred_rf_cal = np.clip(raw_pred_rf * rf_scales, 0.05, 0.50)
    pred_iss_inv = pred_net / (1.0 - pred_rf_cal)
    pred_iss_dir = iss_levels * pred_iss_ratio
    pred_issued = np.maximum(0.5 * pred_iss_dir + 0.5 * pred_iss_inv, pred_net * 1.05)
    pred_refund = pred_issued - pred_net
    pred_rf = np.clip(pred_refund / np.maximum(pred_issued, 1.0), 0.05, 0.50)

    # 3. 单票收益与总收益（GBDT 连续动态单收预测 + 单日假脉冲衰减 + 监管平滑包络约束）
    unit_target = UNIT_PROFIT_BASE[line]
    df['pred_ratio_feat'] = pred_ratio
    df['pred_rf_feat'] = pred_rf
    raw_up_ratio = m_up.predict(df[up_feats]).copy()
    m_up_1d = m_1d_hol & (raw_up_ratio > 1.0)
    raw_up_ratio[m_up_1d] = 1.0 + keep_k * (raw_up_ratio[m_up_1d] - 1.0)

    fut_norm_mask = (df['bk_idx_f'] == 0) & (df['hol_idx_f'] == 0) & (df['d2cny'].abs() > 20)
    norm_ratio_mean = float(raw_up_ratio[fut_norm_mask].mean()) if fut_norm_mask.any() else 1.0
    up_level_target = unit_target / max(norm_ratio_mean, 0.1)

    cny_mask = (df['d2cny'].abs() <= 25) | (df['bk_hol'].astype(str) == '春节') | (df['hol_grp'].astype(str) == '春节')
    hol_mask = (~cny_mask) & ((df['bk_idx_f'] > 0) | (df['hol_idx_f'] > 0))

    if cny_mask.any() and raw_up_ratio[cny_mask].max() > PREMIUM_CNY[line]:
        scale_cny = (PREMIUM_CNY[line] - 1.0) / max(raw_up_ratio[cny_mask].max() - 1.0, 1e-4)
        m_c = cny_mask & (raw_up_ratio > 1.0)
        raw_up_ratio[m_c] = 1.0 + (raw_up_ratio[m_c] - 1.0) * scale_cny

    if hol_mask.any() and raw_up_ratio[hol_mask].max() > PREMIUM_HOL[line]:
        scale_hol = (PREMIUM_HOL[line] - 1.0) / max(raw_up_ratio[hol_mask].max() - 1.0, 1e-4)
        m_h = hol_mask & (raw_up_ratio > 1.0)
        raw_up_ratio[m_h] = 1.0 + (raw_up_ratio[m_h] - 1.0) * scale_hol

    up_levels = np.zeros(n)
    for i in range(n):
        if i <= 30:
            up_levels[i] = up_level_recent
        elif i <= 90:
            beta = (i - 30) / 60.0
            up_levels[i] = (1.0 - beta) * up_level_recent + beta * up_level_target
        else:
            up_levels[i] = up_level_target

    unit_profits = up_levels * raw_up_ratio
    pred_profit = pred_net * unit_profits

    df['tier'] = tier
    df['line'] = line
    df['pred_net_cnt'] = pred_net
    df['pred_issued_cnt'] = pred_issued
    df['pred_refund_cnt'] = pred_refund
    df['pred_refund_rate'] = pred_rf
    df['pred_unit_profit'] = unit_profits
    df['pred_profit'] = pred_profit
    df['level_type'] = level_types

    return df


def ensure_target_table():
    """创建 Hive 结果表（如不存在）"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      `forecast_date`    string COMMENT '预测执行日期(基准日)，如2026-10-15',
      `target_date`      string COMMENT '预测目标日期，2026-10-15至2027-12-31',
      `tier`             string COMMENT '档位:中性/保守/悲观',
      `line`             string COMMENT '产线:纯买/买得到/纯抢/合计',
      `pred_net_cnt`     double COMMENT '预测净票量',
      `pred_issued_cnt`  double COMMENT '预测出票量(由净票量/(1-退票率)倒算)',
      `pred_refund_cnt`  double COMMENT '预测退票量',
      `pred_refund_rate` double COMMENT '预测退票率',
      `pred_unit_profit` double COMMENT '预估单票收益(元/净票)',
      `pred_profit`      double COMMENT '预测营业收益(元)',
      `level_type`       string COMMENT '水位来源:rolling_28d/transition/tier_target'
    )
    COMMENT '火车票2027业务预测结果表(日度滚动重算产出)'
    PARTITIONED BY (`d` string COMMENT '预测分区，同 forecast_date')
    STORED AS ORC
    """
    run_adhoc_sql(ddl, "创建预测结果目标表", exec_mode="HIVE")


def write_results_to_hive(res_df, forecast_date):
    """分批将预测数据写入 Hive 表"""
    print(f"5. 将 {len(res_df)} 行预测结果写入 Hive 表 {TARGET_TABLE} PARTITION (d='{forecast_date}')...")
    
    # 清空已有当天分区（保证幂等重跑）
    clean_sql = f"ALTER TABLE {TARGET_TABLE} DROP IF EXISTS PARTITION (d='{forecast_date}')"
    try:
        run_adhoc_sql(clean_sql, "清理旧分区", exec_mode="HIVE")
    except Exception:
        pass

    cols = ['forecast_date', 'target_date', 'tier', 'line', 'pred_net_cnt',
            'pred_issued_cnt', 'pred_refund_cnt', 'pred_refund_rate',
            'pred_unit_profit', 'pred_profit', 'level_type']

    # 按 tier 分 3 批写入 (每批约 1,772 行)
    for tier in ['中性', '保守', '悲观']:
        sub = res_df[res_df['tier'] == tier]
        val_rows = []
        for _, r in sub.iterrows():
            row_str = (
                f"('{r['forecast_date']}', '{r['target_date']}', '{r['tier']}', '{r['line']}', "
                f"{r['pred_net_cnt']:.2f}, {r['pred_issued_cnt']:.2f}, {r['pred_refund_cnt']:.2f}, "
                f"{r['pred_refund_rate']:.4f}, {r['pred_unit_profit']:.3f}, {r['pred_profit']:.2f}, "
                f"'{r['level_type']}')"
            )
            val_rows.append(row_str)

        rows_joined = ',\n  '.join(val_rows)
        batch_sql = f"""
        INSERT INTO TABLE {TARGET_TABLE} PARTITION (d='{forecast_date}')
        VALUES
          {rows_joined}
        """
        run_adhoc_sql(batch_sql, f"写入 {tier} 档数据 ({len(sub)} 行)", exec_mode="HIVE", timeout=900)
    print(f"  ✓ 写入 Hive 完成，分区 d='{forecast_date}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default='2026-10-15', help='预测基准日期 (YYYY-MM-DD)')
    args = parser.parse_args()
    forecast_date = args.date

    print("=" * 88)
    print(f"火车票 2027 全年业务预测管线（执行日期: {forecast_date}）")
    print("=" * 88)

    # 1 & 2: 加载数据
    df_h, df_fut_base = load_history_and_dim(forecast_date)

    # 3: 训练模型
    ratio_models, iss_models, refund_models, up_models, feats, rf_feats, up_feats, train_df = train_models(df_h)

    # 4: 推理预测
    print(f"4. 运行三档产线预测（预测未来 {len(df_fut_base)} 天）...")
    df_fut = featurize(df_fut_base)
    max_doy_26 = int(train_df[train_df['year'] == 2026]['doy'].max())
    sub_comp = train_df[(train_df['doy'] >= 91) & (train_df['doy'] <= max_doy_26)]

    recent_levels, recent_iss_levels, recent_rf_scales, recent_up_levels = {}, {}, {}, {}
    bases_2026, iss_bases_2026, dyn_params = {}, {}, {}

    for line in LINES:
        h_l = train_df[train_df['line'] == line].sort_values('dt').copy()
        h_l['pred_ratio_feat'] = ratio_models[line].predict(h_l[feats])
        h_l['pred_iss_ratio_feat'] = iss_models[line].predict(h_l[feats])
        h_l['pred_rf_feat'] = refund_models[line].predict(h_l[rf_feats])

        l_comp = sub_comp[sub_comp['line'] == line]
        p25 = float(l_comp[(l_comp['year'] == 2025) & (l_comp['is_peak'] == 1)]['net'].mean())
        p26 = float(l_comp[(l_comp['year'] == 2026) & (l_comp['is_peak'] == 1)]['net'].mean())
        yoy_peak = (p26 - p25) / max(p25, 1.0)
        n25 = float(l_comp[(l_comp['year'] == 2025) & (l_comp['is_peak'] == 0)]['net'].mean())
        n26 = float(l_comp[(l_comp['year'] == 2026) & (l_comp['is_peak'] == 0)]['net'].mean())
        yoy_norm = (n26 - n25) / max(n25, 1.0)
        dyn_params[line] = {
            'yoy_peak': float(np.clip(yoy_peak * 0.85, -0.35, -0.04)),
            'yoy_norm': float(np.clip(yoy_norm * 0.85, -0.45, -0.05)),
            'time_drift': TIME_DRIFT_MAP[line],
        }

        win_df = h_l.iloc[-WIN:].copy()
        norm_mask = (win_df['bk_idx_f'] == 0) & (win_df['hol_idx_f'] == 0)
        norm_df = win_df[norm_mask]
        cal_df = norm_df if (len(norm_df) >= 7 and norm_df['net'].sum() > 0) else win_df

        w_cal = 0.5 ** ((cal_df['dt'].max() - cal_df['dt']).dt.days.values / 12.0)
        level_recent = float(np.sum(cal_df['net'].values * w_cal) / max(np.sum(ratio_models[line].predict(cal_df[feats]) * w_cal), 1.0))
        level_iss_recent = float(np.sum(cal_df['issued'].values * w_cal) / max(np.sum(iss_models[line].predict(cal_df[feats]) * w_cal), 1.0))

        act_rf_norm = float(np.sum(cal_df['refund'].values * w_cal) / max(np.sum(cal_df['issued'].values * w_cal), 1.0))
        pred_rf_norm = float(np.sum(refund_models[line].predict(cal_df[rf_feats]) * cal_df['issued'].values * w_cal) / max(np.sum(cal_df['issued'].values * w_cal), 1.0))
        rf_scale = float(np.clip(act_rf_norm / max(pred_rf_norm, 0.01), 0.85, 1.15))

        actual_norm_up = float(np.sum(cal_df['profit'].values * w_cal) / max(np.sum(cal_df['net'].values * w_cal), 1.0))
        pred_norm_up_r = float(np.sum(up_models[line].predict(cal_df[up_feats]) * w_cal) / max(np.sum(w_cal), 1e-4))
        up_level_recent = float(actual_norm_up / max(pred_norm_up_r, 0.1))

        recent_levels[line] = level_recent
        recent_iss_levels[line] = level_iss_recent
        recent_rf_scales[line] = rf_scale
        recent_up_levels[line] = up_level_recent

        post_shock = h_l[(h_l['dt'] >= '2026-04-01') & (h_l['dt'] <= '2026-09-30')]
        bases_2026[line] = float(post_shock['net'].sum() / max(ratio_models[line].predict(post_shock[feats]).sum(), 1.0))
        iss_bases_2026[line] = float(post_shock['issued'].sum() / max(iss_models[line].predict(post_shock[feats]).sum(), 1.0))

    all_preds = []
    for tier in ['中性', '保守', '悲观']:
        tier_dfs = []
        for line in LINES:
            pred_l = predict_line_tier(
                line, tier, df_fut, ratio_models[line], iss_models[line], refund_models[line], up_models[line],
                feats, rf_feats, up_feats, recent_levels[line], recent_iss_levels[line],
                recent_rf_scales[line], recent_up_levels[line], bases_2026[line], iss_bases_2026[line], dyn_params[line]
            )
            tier_dfs.append(pred_l)
        
        # 汇总为 '合计' 产线 (构造性完全一致)
        cat_df = pd.concat(tier_dfs, ignore_index=True)
        agg_tot = cat_df.groupby('dt').agg(
            pred_net_cnt=('pred_net_cnt', 'sum'),
            pred_issued_cnt=('pred_issued_cnt', 'sum'),
            pred_refund_cnt=('pred_refund_cnt', 'sum'),
            pred_profit=('pred_profit', 'sum'),
        ).reset_index()
        agg_tot['tier'] = tier
        agg_tot['line'] = '合计'
        agg_tot['date'] = agg_tot['dt'].dt.strftime('%Y-%m-%d')
        agg_tot['pred_refund_rate'] = agg_tot['pred_refund_cnt'] / agg_tot['pred_issued_cnt']
        agg_tot['pred_unit_profit'] = agg_tot['pred_profit'] / agg_tot['pred_net_cnt']
        
        # level_type 从纯买继承
        lt_map = dict(zip(tier_dfs[0]['date'], tier_dfs[0]['level_type']))
        agg_tot['level_type'] = agg_tot['date'].map(lt_map)
        
        full_tier = pd.concat([cat_df, agg_tot], ignore_index=True)
        all_preds.append(full_tier)

    final_df = pd.concat(all_preds, ignore_index=True)
    final_df['forecast_date'] = forecast_date
    final_df['target_date'] = final_df['date']

    # 5: 确保 Hive 目标表存在并写入
    ensure_target_table()
    write_results_to_hive(final_df, forecast_date)

    # 6: 汇总打印 2027 全年汇总报表供业务直接审阅
    print("\n" + "=" * 88)
    print("2027 全年预测总览（按档位 × 产线）")
    print("=" * 88)
    f27 = final_df[pd.to_datetime(final_df['target_date']).dt.year == 2027]
    summary = f27.groupby(['tier', 'line']).agg(
        net_yi=('pred_net_cnt', lambda x: x.sum() / 1e8),
        issued_yi=('pred_issued_cnt', lambda x: x.sum() / 1e8),
        profit_yi=('pred_profit', lambda x: x.sum() / 1e8),
        avg_unit_profit=('pred_profit', lambda p: p.sum() / f27.loc[p.index, 'pred_net_cnt'].sum()),
        avg_refund_rate=('pred_refund_cnt', lambda r: r.sum() / f27.loc[r.index, 'pred_issued_cnt'].sum() * 100)
    ).reset_index()

    order_lines = ['纯买', '买得到', '纯抢', '合计']
    summary['line_order'] = summary['line'].map(lambda x: order_lines.index(x))
    summary = summary.sort_values(['tier', 'line_order']).drop(columns='line_order')

    print(f"{'档位':<6}{'产线':<8}{'净票量(亿)':>11}{'出票量(亿)':>11}{'营业收益(亿)':>13}{'平均单收(元)':>13}{'综合退票率':>12}")
    for _, r in summary.iterrows():
        print(f"{r['tier']:<6}{r['line']:<8}{r['net_yi']:>11.2f}{r['issued_yi']:>11.2f}{r['profit_yi']:>13.2f}{r['avg_unit_profit']:>13.2f}{r['avg_refund_rate']:>11.2f}%")


if __name__ == '__main__':
    main()
