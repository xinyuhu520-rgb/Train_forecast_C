# -*- coding: utf-8 -*-
"""
携程国内火车票 2027 全年业务预测 —— Zeus 生产调度脚本 (PySpark 版)

适用平台：Zeus 大数据调度平台 (zeus.ops.ctripcorp.com)
作业类型：AI计算任务 / 其它shell (spark3)
执行账号：trn_mplat_bi
调度时间：每日 09:00 (Cron: 0 0 9 * * ?)
上游依赖：1450983 (cdm_dms_trn_ord_core_idx_sum_di 产出作业，通常 07:47 结束)

数据表架构（全量中台化，无任何私有 tmp 库依赖）：
  输入业务表：dms_traindb.cdm_dms_trn_ord_core_idx_sum_di
  输入维度表：dim_traindb.dim_trn_date_label_df (官方中台日历维表，动态取最新 MAX(d))
  输出结果表：mplat_bi_traindb.adm_trn_forecast_2027_daily_df (中台商业分析正式表)
"""
import sys
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from pyspark.sql import SparkSession

TARGET_TABLE = "mplat_bi_traindb.adm_trn_forecast_2027_daily_df"
SRC_BIZ_TABLE = "dms_traindb.cdm_dms_trn_ord_core_idx_sum_di"
SRC_DIM_TABLE = "dim_traindb.dim_trn_date_label_df"

LINES = ['纯买', '买得到', '纯抢']
WIN = 28
ALL_HOLS = ['元旦', '春节', '清明', '劳动', '端午', '中秋', '国庆']

# 2027 官方调休上班日（dim_trn_date_label_df 中 is_weekend 按纯星期几打标为1，调休上班日需按工作日 is_wknd=0 处理）
MAKEUP_WORKDAYS_2027 = ['2027-01-31', '2027-02-14', '2027-05-08', '2027-09-26', '2027-10-09']
# 由历史 3 天小长假缩短为 1 天单休的节日（由表内 max(holiday_day_index)==1 自动识别，如2027端午/中秋）分产线脉冲保留系数
SINGLE_DAY_PULSE_KEEP = {'纯买': 0.45, '买得到': 0.30, '纯抢': 0.25}

# 三档相对基准的动态偏移量（核心收缩率由模型从数仓最新 2025 vs 2026 实绩动态自学习并随时间与供需演进）
TIER_OFFSETS = {
    '中性': {'纯买': 0.00, '买得到': 0.00, '纯抢': 0.00},
    '保守': {'纯买': -0.03, '买得到': -0.035, '纯抢': -0.05},
    '悲观': {'纯买': -0.06, '买得到': -0.07, '纯抢': -0.10},
}
# 12306 运力供给扩容下，三产线随时间推移（2026Q4 -> 2027Q4）的结构渐进迁移斜率
TIME_DRIFT_MAP = {'纯买': +0.05, '买得到': -0.02, '纯抢': -0.10}

UNIT_PROFIT_BASE = {
    '纯买': 2.91,
    '买得到': 3.56,
    '纯抢': 8.03,
}

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

    # 2027 官方调休上班日：原本为周六/周日，调休上班后按工作日处理 (is_wknd = 0)
    out.loc[out['date_str'].isin(MAKEUP_WORKDAYS_2027), 'is_wknd'] = 0

    # 2027-12-18 ~ 2027-12-31（2028元旦开抢期）：若维表在2028放假公布前尚未填充，则兜底补齐；若维表已填则直接以维表为准
    m_ny28 = (out['date_str'] >= '2027-12-18') & (out['date_str'] <= '2027-12-31') & (out['bk_name'] == '')
    if m_ny28.any():
        out.loc[m_ny28, 'bk_name'] = '元旦开抢第' + ((out.loc[m_ny28, 'dt'] - pd.to_datetime('2027-12-18')).dt.days + 1).astype(str) + '天'
        out.loc[m_ny28, 'bk_idx'] = (out.loc[m_ny28, 'dt'] - pd.to_datetime('2027-12-18')).dt.days + 1

    return out


def featurize(df):
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


def main():
    spark = SparkSession.builder \
        .appName("trn_forecast_2027_daily") \
        .enableHiveSupport() \
        .getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1]:
        forecast_date = sys.argv[1]
    else:
        forecast_date = datetime.now().strftime('%Y-%m-%d')

    yesterday = (datetime.strptime(forecast_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"=== 火车票 2027 预测 Zeus 任务启动: forecast_date={forecast_date}, 历史截至={yesterday} ===")

    dim_max_part = spark.sql(f"SELECT MAX(d) AS md FROM {SRC_DIM_TABLE}").collect()[0]['md']
    print(f"使用中台时间维表: {SRC_DIM_TABLE} 最新分区 d='{dim_max_part}'")

    sql_hist = f"""
    SELECT
      b.business_date AS date,
      b.solution_business_type_cn AS line,
      CAST(sum(b.agent_ticket_cnt) AS double) AS issued,
      CAST(sum(b.refund_ticket_cnt) AS double) AS refund,
      CAST(sum(b.net_agent_ticket_cnt) AS double) AS net,
      CAST(sum(b.profit_amt) AS double) AS profit,
      d.day_of_week AS dow,
      d.is_weekend AS is_wknd,
      d.holiday_name AS hol_name,
      d.holiday_day_name AS hol_day_name,
      d.holiday_day_index AS hol_idx,
      d.booking_day_name AS bk_name,
      d.booking_day_index AS bk_idx,
      d.days_to_cny_eve AS d2cny
    FROM {SRC_BIZ_TABLE} b
    INNER JOIN {SRC_DIM_TABLE} d
      ON b.business_date = d.date_solar
    WHERE b.d BETWEEN '2025-01-01' AND '{yesterday}'
      AND d.d = '{dim_max_part}'
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
    """
    df_h = spark.sql(sql_hist).toPandas()
    df_h['dt'] = pd.to_datetime(df_h['date'])
    df_h['refund_rate'] = df_h['refund'] / df_h['issued']
    df_h = normalize_dim_calendar(df_h)

    sql_fut = f"""
    SELECT
      date_solar AS date,
      day_of_week AS dow,
      is_weekend AS is_wknd,
      holiday_name AS hol_name,
      holiday_day_name AS hol_day_name,
      holiday_day_index AS hol_idx,
      booking_day_name AS bk_name,
      booking_day_index AS bk_idx,
      days_to_cny_eve AS d2cny
    FROM {SRC_DIM_TABLE}
    WHERE d = '{dim_max_part}'
      AND date_solar BETWEEN '2026-10-15' AND '2027-12-31'
    ORDER BY date_solar
    """
    df_f = spark.sql(sql_fut).toPandas()
    df_f['dt'] = pd.to_datetime(df_f['date'])
    df_f = normalize_dim_calendar(df_f)

    lines_res = []
    for line in LINES:
        sub = df_h[df_h['line'] == line].sort_values('dt').copy()
        is_norm = (sub['bk_idx'].fillna(0) == 0) & (sub['hol_idx'].fillna(0) == 0) & (sub['d2cny'].abs() > 20)
        # 【核心优化：35天非假日滚动中位数基线，彻底消除节前单日抢票翘尾与长假对训练分母的污染】
        net_norm_only = sub['net'].copy()
        net_norm_only[~is_norm] = np.nan
        sub['base'] = net_norm_only.rolling(35, min_periods=5).median().shift(1).bfill().ffill()
        sub['ratio'] = sub['net'] / np.maximum(sub['base'], 1.0)

        # 【V4.0 新增：直接出票量去噪中位数基线与比率，用于训练与净票同构的独立出票模型】
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
    # 【V4.0 新增：退票率供需级联特征集，注入当日预测净票比与出票比，捕捉票量与退票率 r=0.872 的强内生耦合】
    rf_feats = feats + ['pred_ratio_feat', 'pred_iss_ratio_feat']
    up_feats = ['dow', 'is_wknd', 'month', 'sin_doy', 'cos_doy', 'sin_dow', 'cos_dow',
                'd2cny', 'bk_idx_f', 'hol_idx_f'] + \
               [f'is_bk_{h}' for h in ALL_HOLS] + [f'is_hol_{h}' for h in ALL_HOLS] + \
               ['pred_ratio_feat', 'pred_rf_feat']

    max_dt = train_df['dt'].max()
    max_doy_26 = int(train_df[train_df['year'] == 2026]['doy'].max())
    sub_comp = train_df[(train_df['doy'] >= 91) & (train_df['doy'] <= max_doy_26)]

    ratio_models, iss_models, refund_models, up_models = {}, {}, {}, {}
    recent_levels, recent_iss_levels, recent_rf_scales, recent_up_levels = {}, {}, {}, {}
    bases_2026, iss_bases_2026, dyn_params = {}, {}, {}

    for line in LINES:
        sub = train_df[train_df['line'] == line].sort_values('dt').copy()
        # 【优化 3：180天半衰期指数样本权重，强化 2026 运力扩容后的新结构趋势】
        sample_w = 0.5 ** ((max_dt - sub['dt']).dt.days.values / 180.0)
        m_r = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_r.fit(sub[feats], sub['ratio'], sample_weight=sample_w)
        ratio_models[line] = m_r

        # 【V4.0 新增：独立出票量 Ratio GBDT 模型，斩断间接倒算公式的同向误差叠加】
        m_iss = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_iss.fit(sub[feats], sub['iss_ratio'], sample_weight=sample_w)
        iss_models[line] = m_iss

        sub['pred_ratio_feat'] = m_r.predict(sub[feats])
        sub['pred_iss_ratio_feat'] = m_iss.predict(sub[feats])

        # 【V4.0 升级：供需级联退票率 GBDT 模型】
        sub_rf = sub.dropna(subset=['refund_rate']).copy()
        w_rf = 0.5 ** ((max_dt - sub_rf['dt']).dt.days.values / 180.0)
        m_rf = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, random_state=42)
        m_rf.fit(sub_rf[rf_feats], sub_rf['refund_rate'], sample_weight=w_rf)
        refund_models[line] = m_rf

        # 【优化 1：单票收益 GBDT 供需联动连续动态回归模型】
        sub['pred_rf_feat'] = m_rf.predict(sub[rf_feats])
        sub_up = sub.dropna(subset=['up_ratio']).copy()
        w_up = 0.5 ** ((max_dt - sub_up['dt']).dt.days.values / 180.0)
        m_up = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05, max_depth=4, min_samples_leaf=4, random_state=42)
        m_up.fit(sub_up[up_feats], sub_up['up_ratio'], sample_weight=w_up)
        up_models[line] = m_up

        # 【优化 3：从数仓实绩动态自学习各产线在「高峰抢票态」与「日常平水态」的真实同比收缩率】
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

        # 【优化 2 & V4.0 升级：假日峰值剥离 + 12天半衰期指数加权动态定标 (净票/出票/退票率/单收四维同步定标)】
        win_df = sub.iloc[-WIN:].copy()
        norm_mask = (win_df['bk_idx_f'] == 0) & (win_df['hol_idx_f'] == 0)
        norm_df = win_df[norm_mask]
        cal_df = norm_df if (len(norm_df) >= 7 and norm_df['net'].sum() > 0) else win_df
        de_holiday_tag = f"去噪平水日({len(norm_df)}天)" if len(norm_df) >= 7 else f"全量滑动窗口({len(win_df)}天)"

        w_cal = 0.5 ** ((cal_df['dt'].max() - cal_df['dt']).dt.days.values / 12.0)
        level_recent = float(np.sum(cal_df['net'].values * w_cal) / max(np.sum(m_r.predict(cal_df[feats]) * w_cal), 1.0))
        level_iss_recent = float(np.sum(cal_df['issued'].values * w_cal) / max(np.sum(m_iss.predict(cal_df[feats]) * w_cal), 1.0))

        act_rf_norm = float(np.sum(cal_df['refund'].values * w_cal) / max(np.sum(cal_df['issued'].values * w_cal), 1.0))
        pred_rf_norm = float(np.sum(m_rf.predict(cal_df[rf_feats]) * cal_df['issued'].values * w_cal) / max(np.sum(cal_df['issued'].values * w_cal), 1.0))
        rf_scale = float(np.clip(act_rf_norm / max(pred_rf_norm, 0.01), 0.85, 1.15))

        actual_norm_up = float(np.sum(cal_df['profit'].values * w_cal) / max(np.sum(cal_df['net'].values * w_cal), 1.0))
        pred_norm_up_r = float(np.sum(m_up.predict(cal_df[up_feats]) * w_cal) / max(np.sum(w_cal), 1e-4))
        up_level_recent = float(actual_norm_up / max(pred_norm_up_r, 0.1))

        recent_levels[line] = level_recent
        recent_iss_levels[line] = level_iss_recent
        recent_rf_scales[line] = rf_scale
        recent_up_levels[line] = up_level_recent

        post_shock = sub[(sub['dt'] >= '2026-04-01') & (sub['dt'] <= '2026-09-30')]
        bases_2026[line] = float(post_shock['net'].sum() / max(m_r.predict(post_shock[feats]).sum(), 1.0))
        iss_bases_2026[line] = float(post_shock['issued'].sum() / max(m_iss.predict(post_shock[feats]).sum(), 1.0))
        print(f"产线 [{line}] V4.0四维定标完成: {de_holiday_tag}, 净票水位={level_recent:.1f}, 出票水位={level_iss_recent:.1f}, 退票率校准={rf_scale:.3f}(近期退票率={act_rf_norm*100:.2f}%)")

    df_fut = featurize(df_f)
    # 从中台维表 dim_trn_date_label_df 自动识别 2027 年放假天数为 1 天的单日假（如端午、中秋）
    hol_27_max = df_fut[(df_fut['dt'].dt.year == 2027) & (df_fut['hol_grp'] != '无')].groupby('hol_grp')['hol_idx_f'].max()
    single_day_hols_2027 = list(hol_27_max[hol_27_max == 1].index)
    print(f"从中台维表自动识别 2027 单日假(放假1天): {single_day_hols_2027}")
    all_preds = []

    for tier in ['中性', '保守', '悲观']:
        tier_dfs = []
        for line in LINES:
            df = df_fut.copy()
            pred_ratio = ratio_models[line].predict(df[feats]).copy()
            pred_iss_ratio = iss_models[line].predict(df[feats]).copy()

            # 【2027 官方节假日脉冲修正：对维表中放假天数为1天的节日(端午/中秋)，按产线衰减中长途与抢票超额脉冲】
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

            # 【V4.0 退票率预测：近期平水退票率定标 (0~30天) 平滑过渡至长期稳态 (30~90天)】
            raw_pred_rf = refund_models[line].predict(df[rf_feats])
            rf_scale_recent = recent_rf_scales[line]

            level_recent = recent_levels[line]
            level_iss_recent = recent_iss_levels[line]
            up_level_recent = recent_up_levels[line]
            unit_target = UNIT_PROFIT_BASE[line]

            # 【三维动态 TIERS 引擎：滚动自校准 × 时间渐进迁移 prog(t) × 当日供需紧张度 tau(t)】
            r_span = max(pred_ratio.max() - pred_ratio.min(), 1e-4)
            rf_span = max(raw_pred_rf.max() - raw_pred_rf.min(), 1e-4)
            tau = np.clip(0.5 * (pred_ratio - pred_ratio.min()) / r_span + 0.5 * (raw_pred_rf - raw_pred_rf.min()) / rf_span, 0.0, 1.0)

            n = len(df)
            prog = np.linspace(0.0, 1.0, n)
            r_norm_t = dyn_params[line]['yoy_norm'] + prog * dyn_params[line]['time_drift'] + TIER_OFFSETS[tier][line]
            r_peak_t = dyn_params[line]['yoy_peak'] + 0.5 * prog * dyn_params[line]['time_drift'] + TIER_OFFSETS[tier][line]
            dynamic_tier_rates = (1.0 - tau) * r_norm_t + tau * r_peak_t

            levels = np.zeros(n)
            iss_levels = np.zeros(n)
            rf_scales = np.zeros(n)
            level_types = []
            for i in range(n):
                level_tier_i = bases_2026[line] * (1.0 + dynamic_tier_rates[i])
                iss_level_tier_i = iss_bases_2026[line] * (1.0 + dynamic_tier_rates[i])
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

            pred_net = levels * pred_ratio
            pred_rf_cal = np.clip(raw_pred_rf * rf_scales, 0.05, 0.50)

            # 【V4.0 核心：直接出票模型 pred_iss_dir 与校准退票率倒算模型 pred_iss_inv 双轨等权融合，斩断同向误差放大】
            pred_iss_inv = pred_net / (1.0 - pred_rf_cal)
            pred_iss_dir = iss_levels * pred_iss_ratio
            pred_issued = np.maximum(0.5 * pred_iss_dir + 0.5 * pred_iss_inv, pred_net * 1.05)
            pred_refund = pred_issued - pred_net
            pred_rf = np.clip(pred_refund / np.maximum(pred_issued, 1.0), 0.05, 0.50)

            # 【优化 1 (升级版)：GBDT 连续动态单收预测 + 监管平滑包络约束】
            df['pred_ratio_feat'] = pred_ratio
            df['pred_rf_feat'] = pred_rf
            raw_up_ratio = up_models[line].predict(df[up_feats]).copy()
            m_up_1d = m_1d_hol & (raw_up_ratio > 1.0)
            raw_up_ratio[m_up_1d] = 1.0 + keep_k * (raw_up_ratio[m_up_1d] - 1.0)

            fut_norm_mask = (df['bk_idx_f'] == 0) & (df['hol_idx_f'] == 0) & (df['d2cny'].abs() > 20)
            norm_ratio_mean = float(raw_up_ratio[fut_norm_mask].mean()) if fut_norm_mask.any() else 1.0
            up_level_target = unit_target / max(norm_ratio_mean, 0.1)

            # 对春运及小长假峰值施加平滑比例压缩包络，既严格满足监管上限，又100%保留每日自然爬坡回落波形
            cny_mask = (df['d2cny'].abs() <= 25) | (df['bk_hol'] == '春节') | (df['hol_grp'] == '春节')
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
            tier_dfs.append(df)

        cat_df = pd.concat(tier_dfs, ignore_index=True)
        sum_cols = ['pred_net_cnt', 'pred_issued_cnt', 'pred_refund_cnt', 'pred_profit']
        agg_tot = cat_df.groupby('dt')[sum_cols].sum().reset_index()
        agg_tot['tier'] = tier
        agg_tot['line'] = '合计'
        agg_tot['date'] = agg_tot['dt'].dt.strftime('%Y-%m-%d')
        agg_tot['pred_refund_rate'] = agg_tot['pred_refund_cnt'] / agg_tot['pred_issued_cnt']
        agg_tot['pred_unit_profit'] = agg_tot['pred_profit'] / agg_tot['pred_net_cnt']
        lt_map = dict(zip(tier_dfs[0]['date'], tier_dfs[0]['level_type']))
        agg_tot['level_type'] = agg_tot['date'].map(lt_map)
        full_tier = pd.concat([cat_df, agg_tot], ignore_index=True)
        all_preds.append(full_tier)

    final_df = pd.concat(all_preds, ignore_index=True)
    final_df['forecast_date'] = forecast_date
    final_df['target_date'] = final_df['date']

    res_cols = ['forecast_date', 'target_date', 'tier', 'line',
                'pred_net_cnt', 'pred_issued_cnt', 'pred_refund_cnt',
                'pred_refund_rate', 'pred_unit_profit', 'pred_profit', 'level_type']
    out_df = final_df[res_cols].copy()

    # 6. 转为 Spark DataFrame 原生写入中台预测正式表 (纯 DML，避免触发平台 DDLHook 限制)
    spark_out = spark.createDataFrame(out_df)
    spark_out.createOrReplaceTempView("tmp_forecast_view")
    spark.sql(f"""
    INSERT OVERWRITE TABLE {TARGET_TABLE} PARTITION (d='{forecast_date}')
    SELECT
      forecast_date, target_date, tier, line,
      pred_net_cnt, pred_issued_cnt, pred_refund_cnt,
      pred_refund_rate, pred_unit_profit, pred_profit, level_type
    FROM tmp_forecast_view
    """)
    print(f"=== 成功写入中台目标表 {TARGET_TABLE} 分区 d='{forecast_date}'，行数: {len(out_df)} ===")
    spark.stop()


if __name__ == '__main__':
    main()
