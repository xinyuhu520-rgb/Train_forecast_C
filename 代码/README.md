# 火车票 2027 业务大盘预测 · 代码工程目录与维护指南（V4.0 生产正式版）

> **最新更新时间**：2026-11-10（已接入 V4.0 出票/退票双轨解耦算法与国务院 2027 正式节假日排期）  
> **生产调度平台**：Zeus 大数据调度平台（执行账号：`trn_mplat_bi`）  
> **正式中台表**：
> - 稳定日历维表：`dim_traindb.dim_trn_date_label_df`（2027 官方节假日与开抢期已全量生效）
> - 纯预测基准表：`mplat_bi_traindb.adm_trn_forecast_2027_daily_df`（由 Zeus 任务 `1618901` 每日产出）
> - 实绩与预测一体化表：`mplat_bi_traindb.adm_trn_ord_forecast_2027_combined_di`（由 Zeus 任务 `1619716` 每日合成，直连 ArtNova 看板）

---

## 一、代码文件清单与职责索引

| 序号 | 文件名 | 类型 | 核心职责 | 输入表 / 数据源 | 输出表 / 产物 |
|---|---|---|---|---|---|
| **01** | `zeus_forecast_2027.py` | PySpark (Zeus任务1) | **Zeus 生产算法主程序 (Job `1618901`)**：每日 09:00 自动拉取最新实绩与中台日历维表，执行 V4.0 四模型训练、三维动态 `TIERS` 演进、2027 单日假脉冲衰减与双轨出票融合，写入中台预测表 | `dms_traindb.cdm_dms_trn_ord_core_idx_sum_di`<br>`dim_traindb.dim_trn_date_label_df` | `mplat_bi_traindb.adm_trn_forecast_2027_daily_df` |
| **02** | `zeus_job_runner.sh` | Shell (Zeus任务1入口) | **Zeus 任务 `1618901` 启动脚本**：从 HDFS 拉取最新 `zeus_forecast_2027.py` 并提交 `spark-submit` | HDFS 资源文件 `zeus_forecast_2027.py` | 驱动 Job `1618901` 执行 |
| **03** | `zeus_combine_forecast_actual.sql` | Hive SQL (Zeus任务2) | **Zeus 实绩与预测一体化合成脚本 (Job `1619716`)**：依赖 `1618901`，将 2026+ 真实数据与三档预测数据做全外连接，生成 ArtNova 单主表直连宽表 | `mplat_bi_traindb.adm_trn_forecast_2027_daily_df`<br>`dms_traindb.cdm_dms_trn_ord_core_idx_sum_di` | `mplat_bi_traindb.adm_trn_ord_forecast_2027_combined_di` |
| **04** | `run_forecast_2027_hive.py` | Python (本地复现版) | **本地 Adhoc 驱动的预测主脚本**：与 `zeus_forecast_2027.py` 保持 100% 相同的 V4.0 算法与日历逻辑，用于本地离线回测与调试 | `dms_traindb.cdm_dms_trn_ord_core_idx_sum_di`<br>`dim_traindb.dim_trn_date_label_df` | `mplat_bi_traindb.adm_trn_forecast_2027_daily_df` |
| **05** | `create_dim_date_2025_2027.sql` | Hive SQL (规则存档) | 2025–2027 日历规则参考 SQL（已同步 2027 国务院正式放假区间；生产环境直接读取已生效的 `dim_traindb.dim_trn_date_label_df`） | `dim_traindb.dim_trn_date_label_df` | 规则备查与沙箱验证 |

---

## 二、生产双任务解耦流水线（Zeus 自动化架构）

```
               上游任务 1450983 就绪 (每日约 07:47 产出 Ctrip.Train 实际数据)
                                 │
                                 ▼ 09:00 准时启动
┌────────────────────────────────────────────────────────────────────────┐
│ 【Zeus 任务 1：算法预测任务】(Job ID: 1618901)                           │
│  • 类型：AI计算任务 / spark3 (PySpark)                                  │
│  • 账号：trn_mplat_bi                                                  │
│  • 脚本：zeus_forecast_2027.py                                         │
│  • 维表：dim_traindb.dim_trn_date_label_df (自动取最新 MAX(d))          │
│  • 产出：mplat_bi_traindb.adm_trn_forecast_2027_daily_df (5,316行/日)   │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ 任务 1 成功后自动唤醒
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 【Zeus 任务 2：实绩与预测合成任务】(Job ID: 1619716)                     │
│  • 类型：Hive SQL                                                      │
│  • 账号：trn_mplat_bi                                                  │
│  • 脚本：zeus_combine_forecast_actual.sql                              │
│  • 产出：mplat_bi_traindb.adm_trn_ord_forecast_2027_combined_di         │
│  • 下游：ArtNova 看板 (609ff758-bfa2-4386-8398-1f611b8daf1a) 自动刷新   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 三、四期核心算法演进与调参指南（`zeus_forecast_2027.py`）

所有的核心配置集中在 `zeus_forecast_2027.py` 第 23 ~ 56 行：

### 1. 2027 官方节假日与单日假（3天变1天）脉冲衰减配置
- **日历源**：直接读取 `dim_traindb.dim_trn_date_label_df` 最新分区 `MAX(d)`，无需维护硬编码放假起止日期；
- **单日假自动识别与脉冲衰减**：代码自动从维表识别 2027 年 `max(holiday_day_index) == 1` 的单日假（**端午 `2027-06-09`、中秋 `2027-09-15`**，均由历史 3 天小长假缩减为周三单休 1 天），并按产线衰减高于平水基线的超额脉冲与单收溢价：
  
  ```python
  # 2027 官方调休上班日（维表中 is_weekend=1，代码中强制修正为工作日 is_wknd=0）
  MAKEUP_WORKDAYS_2027 = ['2027-01-31', '2027-02-14', '2027-05-08', '2027-09-26', '2027-10-09']
  # 1天单休假分产线超额脉冲保留系数（短途直购保留45%，中转保留30%，长途抢票仅保留25%）
  SINGLE_DAY_PULSE_KEEP = {'纯买': 0.45, '买得到': 0.30, '纯抢': 0.25}
  ```

### 2. 三维动态 `TIERS` 结构演进参数（V3.0）
```python
# 三档相对基准偏移量（基础同比收缩率由模型从数仓 2025 vs 2026 实绩动态自学习）
TIER_OFFSETS = {
    '中性': {'纯买': 0.00, '买得到': 0.00, '纯抢': 0.00},
    '保守': {'纯买': -0.03, '买得到': -0.035, '纯抢': -0.05},
    '悲观': {'纯买': -0.06, '买得到': -0.07, '纯抢': -0.10},
}
# 12306 运力供给扩容下，三产线随时间推移（2026Q4 -> 2027Q4）的结构渐进迁移斜率
TIME_DRIFT_MAP = {'纯买': +0.05, '买得到': -0.02, '纯抢': -0.10}
```

### 3. 单票收益基准与监管平滑包络上限（V2.0）
```python
UNIT_PROFIT_BASE = {'纯买': 2.91, '买得到': 3.56, '纯抢': 8.03}
PREMIUM_CNY = {'纯买': 1.08, '买得到': 1.40, '纯抢': 1.35}
PREMIUM_HOL = {'纯买': 1.03, '买得到': 1.10, '纯抢': 1.15}
```

### 4. 出票量与退票率双轨解耦动态定标（V4.0）
- **独立出票模型 (`m_iss`)**：与净票模型 (`m_r`) 对称建模，基于 35 天非假日出票中位数基线独立预测出票量 `pred_iss_dir`；
- **供需级联退票率 (`m_rf`) + 近期退票率校准 (`rf_scale`)**：将当日预测净票比与出票比作为特征输入 `m_rf`，并用近 28 天平水日（12天半衰期指数加权）校准近期退票率中枢；
- **双轨等权融合**：`pred_issued = 0.5 * pred_iss_dir + 0.5 * pred_iss_inv`，斩断单一倒算公式 `Net / (1 - RF)` 的同向误差放大效应，使近 7 日大盘出票准确率提升至 **98.36%**（WAPE **1.64%**）。
