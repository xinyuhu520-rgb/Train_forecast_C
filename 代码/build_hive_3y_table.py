# -*- coding: utf-8 -*-
"""
在 Hive 上建 2025-2027 三年时间维度表（非分区表，便于直接 join）

目标表：tmp_bu_trn.dim_trn_date_label_2025_2027_tmp
Hive 账号：buadm_trn（对 tmp_bu_trn 有 ALL 权限）

做法：
  1. 建表，15 个业务字段 + src_type 标记（字段名/类型/注释对齐线上 dim_trn_date_label_df）
  2. 灌数用 INSERT ... SELECT，**从线上表直接读**，不写 1095 行字面值：
       - 纯日历字段（10 个）+ 农历 + days_to_cny_eve  → 线上原值（已预填到 2047）
       - holiday_* / booking_* 5 个字段               → COALESCE(线上值, 本地映射值)
     线上有值就用线上的（2025/2026），线上为空才用我生成的（2027 全年 + 2026-12-18~31
     的 2027 元旦开抢期）。COALESCE 顺序保证绝不覆盖线上真实值。
  3. 映射值来自 dim_date_2025_2027_temp.csv —— 该文件已过 9 项自检、且与业务表
     关联 1095/1950 行零缺失，是已核验的数据

执行通道：adhoc --exec-mode HIVE（已验证可跑 DDL/DML；默认的 STARROCKS_HIVE 会被
          引擎追加 limit 而打挂 DDL）
"""
import json
import subprocess
import pandas as pd

SCRIPTS = '/Users/xinyhu/.agents/skills/adhoc-artnova-data-access/scripts'
OUT_DIR = '/Users/xinyhu/Desktop/预算预测项目'
CSV = f'{OUT_DIR}/dim_date_2025_2027_temp.csv'

DB = 'tmp_bu_trn'
TABLE = 'dim_trn_date_label_2025_2027_tmp'
FQN = f'{DB}.{TABLE}'
SRC = 'dim_traindb.dim_trn_date_label_df'

# 需要本地补值的日期范围：2027 全年 + 2026-12-18~31（2027元旦跨年开抢期）
PATCH_FROM = '2026-12-18'


def run_sql(sql: str, label: str, timeout=900):
    """通过 adhoc HIVE 引擎执行 SQL"""
    r = subprocess.run(
        ['python3', 'adhoc_sql.py', '--browser', 'chrome', '--exec-mode', 'HIVE',
         '--insecure', '--limit-disable', '--format', 'json', '--sql', sql],
        cwd=SCRIPTS, capture_output=True, text=True, timeout=timeout)
    txt = r.stdout
    k = txt.find('{')
    if k < 0:
        raise RuntimeError(f'[{label}] 执行失败\nstdout={txt[:800]}\nstderr={r.stderr[:800]}')
    d = json.loads(txt[k:txt.rindex('}') + 1])
    if d.get('status') != 'FINISHED':
        raise RuntimeError(f"[{label}] 未完成: {str(d)[:600]}")
    print(f'  ✓ {label}')
    return d


def sql_str(v):
    """转 SQL 字面值，None/NaN → NULL，单引号转义"""
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return 'NULL'
    return "'" + str(v).replace("'", "''") + "'"


def sql_int(v):
    if v is None or pd.isna(v):
        return 'NULL'
    return str(int(v))


def build_patch_values(df):
    """只为「线上为空、需本地补」的日期生成映射行，减小 SQL 体积"""
    p = df[df['date_solar'] >= PATCH_FROM].copy()
    rows = []
    for _, r in p.iterrows():
        has = any(pd.notna(r[c]) for c in
                  ['holiday_name', 'holiday_day_name', 'holiday_day_index',
                   'booking_day_name', 'booking_day_index'])
        if not has:
            continue
        rows.append(
            f"SELECT {sql_str(r['date_solar'])} AS ds, {sql_str(r['holiday_name'])} AS hn, "
            f"{sql_str(r['holiday_day_name'])} AS hdn, {sql_int(r['holiday_day_index'])} AS hdi, "
            f"{sql_str(r['booking_day_name'])} AS bdn, {sql_int(r['booking_day_index'])} AS bdi")
    return rows


DDL = f"""CREATE TABLE IF NOT EXISTS {FQN} (
  `date_solar` string COMMENT '阳历日期',
  `date_lunar` string COMMENT '农历日期',
  `day_of_week` int COMMENT '星期几(1-7)',
  `day_of_month` int COMMENT '月内第几天',
  `day_of_year` int COMMENT '年内第几天',
  `week_of_year` int COMMENT '年内第几周',
  `firstday_of_week` string COMMENT '所在周的第一天',
  `lastday_of_week` string COMMENT '所在周的最后一天',
  `is_weekend` int COMMENT '是否周末(1:是, 0:否)',
  `holiday_name` string COMMENT '假期名称(如:国庆假期)',
  `holiday_day_name` string COMMENT '假期出行当天名称(如:国庆出行第1天)',
  `holiday_day_index` int COMMENT '假期出行第X天',
  `booking_day_name` string COMMENT '假期开抢当天名称(如:国庆开抢第1天)',
  `booking_day_index` int COMMENT '假期开抢第X天',
  `days_to_cny_eve` int COMMENT '距离除夕天数',
  `src_type` string COMMENT '来源:online=线上原值, est=规则推算(2027及2026-12开抢期)'
)
COMMENT '火车票日期维度表 2025-2027(临时):2025/2026取线上原值,2027按放假规则推算,非分区'
STORED AS ORC"""


def main():
    df = pd.read_csv(CSV)
    print('=' * 86)
    print(f'在 Hive 建三年时间维度表 → {FQN}')
    print('=' * 86)
    print(f'  源表 {SRC}（纯日历字段+农历+d2cny 取线上原值）')
    print(f'  本地补值 {CSV}')

    patch = build_patch_values(df)
    print(f'  需本地补值的日期 {len(patch)} 天（2027 全年节假日/开抢期 + 2026-12 跨年开抢期）')

    print('\n[1/4] 建表')
    run_sql(DDL, f'CREATE TABLE {FQN}')

    print('\n[2/4] 灌数（INSERT OVERWRITE ... SELECT，从线上表读取）')
    patch_sql = '\n  UNION ALL '.join(patch)
    insert = f"""INSERT OVERWRITE TABLE {FQN}
SELECT
  s.date_solar, s.date_lunar, s.day_of_week, s.day_of_month, s.day_of_year,
  s.week_of_year, s.firstday_of_week, s.lastday_of_week, s.is_weekend,
  COALESCE(NULLIF(s.holiday_name,''),      p.hn)  AS holiday_name,
  COALESCE(NULLIF(s.holiday_day_name,''),  p.hdn) AS holiday_day_name,
  COALESCE(s.holiday_day_index,            p.hdi) AS holiday_day_index,
  COALESCE(NULLIF(s.booking_day_name,''),  p.bdn) AS booking_day_name,
  COALESCE(s.booking_day_index,            p.bdi) AS booking_day_index,
  s.days_to_cny_eve,
  CASE WHEN COALESCE(NULLIF(s.holiday_name,''), NULLIF(s.booking_day_name,'')) IS NULL
            AND COALESCE(p.hn, p.bdn) IS NOT NULL
       THEN 'est' ELSE 'online' END AS src_type
FROM (
  SELECT * FROM {SRC}
  WHERE d = (SELECT MAX(d) FROM {SRC})
    AND date_solar BETWEEN '2025-01-01' AND '2027-12-31'
) s
LEFT JOIN (
  {patch_sql}
) p ON s.date_solar = p.ds"""
    run_sql(insert, f'INSERT OVERWRITE {len(df)} 行', timeout=1800)

    print('\n[3/4] 校验行数与分年覆盖')
    q = f"""SELECT substr(date_solar,1,4) yr, count(*) days,
       sum(if(holiday_name   is not null and holiday_name<>'',1,0)) hol,
       sum(if(booking_day_name is not null and booking_day_name<>'',1,0)) bk,
       sum(if(src_type='est',1,0)) est,
       sum(if(days_to_cny_eve is null,1,0)) d2c_null,
       count(distinct date_solar) uniq
FROM {FQN} GROUP BY substr(date_solar,1,4) ORDER BY yr"""
    d = run_sql(q, '分年统计')
    print(f"\n  {'年':<6}{'天数':>6}{'holiday':>9}{'booking':>9}{'est行':>7}{'d2cny空':>9}{'日期唯一':>9}")
    for r in d['rows']:
        print(f'  {r[0]:<6}{r[1]:>6}{r[2]:>9}{r[3]:>9}{r[4]:>7}{r[5]:>9}{r[6]:>9}')

    print('\n[4/4] 与本地 CSV 逐行对比（确保 Hive 表内容 == 已核验的 CSV）')
    q2 = f"""SELECT date_solar, holiday_name, holiday_day_name, holiday_day_index,
       booking_day_name, booking_day_index, days_to_cny_eve, is_weekend
FROM {FQN} ORDER BY date_solar"""
    d2 = run_sql(q2, '拉回全表')
    hv = pd.DataFrame(d2['rows'], columns=d2['columns']).replace('null', None)
    loc = df[['date_solar', 'holiday_name', 'holiday_day_name', 'holiday_day_index',
              'booking_day_name', 'booking_day_index', 'days_to_cny_eve', 'is_weekend']].copy()
    print(f'  Hive {len(hv)} 行  vs  本地 {len(loc)} 行')
    diff = 0
    for c in loc.columns:
        a = hv[c].astype(str).replace({'None': '', 'nan': '', 'null': ''})
        b = loc[c].astype(str).replace({'None': '', 'nan': '', 'nan.0': ''})
        if c in ('holiday_day_index', 'booking_day_index', 'days_to_cny_eve', 'is_weekend'):
            a = pd.to_numeric(hv[c], errors='coerce')
            b = pd.to_numeric(loc[c], errors='coerce')
            ne = ~((a == b) | (a.isna() & b.isna()))
        else:
            ne = a.str.strip() != b.str.strip()
        n = int(ne.sum())
        diff += n
        print(f"    {c:<20}{'一致 ✓' if n == 0 else f'{n} 行不一致 ✗'}")
    print(f"\n  {'全字段逐行一致 ✓' if diff == 0 else f'★ 共 {diff} 处不一致'}")
    print(f'\n  表名: {FQN}')
    return FQN


if __name__ == '__main__':
    main()
