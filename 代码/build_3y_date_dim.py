# -*- coding: utf-8 -*-
"""
生成 2025-2027 三年临时时间维度表 —— schema 与 dim_traindb.dim_trn_date_label_df 完全一致

数据来源分三段：
  2025 全年   —— 线上原值（holiday 28 天 / booking 112 天，已填齐）
  2026 全年   —— 线上原值 + 补 2026-12-18~31（线上为空，见下）
  2027 全年   —— 规则生成（线上 holiday/booking 全空，等官方公布）

★ 2026-12-18~31 为什么要补：
  元旦开抢期跨年。线上口径（已核对 2025-12 实值）是上年 12/18~12/31 填
  「元旦开抢第1~14天」且 holiday_name 留空，次年 1/1~1/3 接第 15~17 天。
  线上 2025-12 已这样填（给 2026 元旦），但 2026-12 还是空的（给 2027 元旦），
  否则三年表里 2027 元旦的开抢期会从 1/1 的「第15天」突然开始。

⚠️ 边界说明：表首 2025-01-01~01-03 的开抢期起点落在 2024-12（区间外），
   因此 booking_day_index 从中段开始，属取数区间的固有边界，非缺陷。

2027 规则与 build_2027_date_dim.py 完全一致，直接复用其函数，避免两处规则漂移。
"""
from datetime import date, timedelta
import pandas as pd

from build_2027_date_dim import (
    COLUMNS, INT_COLS, BOOKING_LEAD, OUT_DIR, WD,
    build_holiday_plans, fetch_online_base, fill_holiday_fields,
)

YEARS = (2025, 2026, 2027)
NY_PATCH_YEAR = 2026          # 需要补元旦跨年开抢期的那一年的 12 月


def patch_newyear_booking(df: pd.DataFrame, year: int) -> tuple[pd.DataFrame, int]:
    """补 <year>-12-18~12-31 的次年元旦开抢期（线上口径：holiday_name 留空）"""
    start = date(year, 12, 31) - timedelta(days=BOOKING_LEAD - 1)   # 12/18
    df = df.copy()
    n = 0
    for i in range(BOOKING_LEAD):
        day = (start + timedelta(days=i)).isoformat()
        m = df['date_solar'] == day
        if not m.any():
            continue
        cur = df.loc[m, 'booking_day_name'].iloc[0]
        if pd.notna(cur) and str(cur) not in ('', 'null'):
            continue                                   # 线上已填则不动
        df.loc[m, 'booking_day_name'] = f'元旦开抢第{i+1}天'
        df.loc[m, 'booking_day_index'] = i + 1
        n += 1
    return df, n


def main():
    plans = build_holiday_plans()

    print('=' * 88)
    print('生成 2025-2027 三年临时时间维度表')
    print('=' * 88)

    parts, report = [], []
    for y in YEARS:
        base = fetch_online_base(y)
        src = '线上原值'
        if y == 2027:
            base = fill_holiday_fields(base, plans)    # 线上全空 → 规则生成
            src = '规则生成'
        if y == NY_PATCH_YEAR:
            base, n = patch_newyear_booking(base, y)
            if n:
                src += f' + 补 12/18~12/31 元旦开抢({n}天)'
        parts.append(base)
        report.append((y, len(base), src))
        print(f'  {y}  {len(base):>4} 行   {src}')

    df = pd.concat(parts, ignore_index=True).sort_values('date_solar').reset_index(drop=True)
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    df = df[COLUMNS]

    out = f'{OUT_DIR}/dim_date_2025_2027_temp.csv'
    df.to_csv(out, index=False, encoding='utf-8-sig')

    # ---------------- 自检 ----------------
    dt = pd.to_datetime(df['date_solar'])
    print('\n' + '=' * 88)
    print('自检')
    print('=' * 88)
    chk = [
        ('字段名与顺序同线上', list(df.columns) == COLUMNS),
        ('行数 1095 (365*3)', len(df) == 1095),
        ('日期唯一且全程连续', df['date_solar'].nunique() == len(df)
            and dt.diff().dropna().dt.days.eq(1).all()),
        ('区间 2025-01-01 ~ 2027-12-31',
            df['date_solar'].iloc[0] == '2025-01-01' and df['date_solar'].iloc[-1] == '2027-12-31'),
        ('is_weekend 与星期一致',
            (df['is_weekend'] == (dt.dt.dayofweek >= 5).astype(int)).all()),
        ('date_lunar 无缺失', df['date_lunar'].notna().all()),
        ('days_to_cny_eve 无缺失', df['days_to_cny_eve'].notna().all()),
    ]
    # 三个除夕均为 0
    eves = {'2025-01-28': 2025, '2026-02-16': 2026, '2027-02-05': 2027}
    chk.append(('三年除夕 d2cny 均为 0',
                all(int(df.loc[df['date_solar'] == k, 'days_to_cny_eve'].iloc[0]) == 0
                    for k in eves)))
    # 元旦跨年开抢期连续性：每年 12/18~次年 1/3 应为 1..17
    ok = True
    for y in (2025, 2026):
        seq = []
        for i in range(17):
            day = (date(y, 12, 18) + timedelta(days=i)).isoformat()
            r = df[df['date_solar'] == day]
            if len(r) and pd.notna(r['booking_day_index'].iloc[0]):
                seq.append(int(r['booking_day_index'].iloc[0]))
        if seq != list(range(1, 18)):
            ok = False
    chk.append(('元旦开抢期跨年连续 1..17（2025-12、2026-12）', ok))
    for n, v in chk:
        print(f"  {'✓' if v else '✗'}  {n}")
    print(f"\n  {'全部通过 ✓' if all(v for _, v in chk) else '★ 有未通过项'}")

    # ---------------- 分年覆盖 ----------------
    print('\n' + '=' * 88)
    print('分年字段覆盖')
    print('=' * 88)
    df['yr'] = dt.dt.year
    print(f"{'年':<6}{'天数':>5}{'holiday_name':>14}{'booking_day_name':>18}  来源")
    for y, cnt, src in report:
        g = df[df['yr'] == y]
        print(f"{y:<6}{len(g):>5}{g['holiday_name'].notna().sum():>14}"
              f"{g['booking_day_name'].notna().sum():>18}  {src}")

    print('\n各年节假日明细:')
    for y in YEARS:
        g = df[(df['yr'] == y) & df['holiday_name'].notna()].copy()
        g['hol'] = g['holiday_name'].str.replace('假期', '', regex=False)
        segs = []
        for nm, gg in g.groupby('hol', sort=False):
            segs.append(f"{nm} {gg['date_solar'].min()[5:]}~{gg['date_solar'].max()[5:]}({len(gg)}天)")
        print(f"  {y}: " + '; '.join(segs))

    print(f'\n  已存: {out}')
    print(f'  {len(df)} 行 × {len(COLUMNS)} 字段')
    return df


if __name__ == '__main__':
    main()
