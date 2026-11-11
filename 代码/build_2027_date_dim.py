# -*- coding: utf-8 -*-
"""
生成 2027 年临时时间维度表 —— schema 与 dim_traindb.dim_trn_date_label_df 完全一致

做法：以线上表的 2027 行为底座（纯日历字段 + 农历已预填到 2047），
      只填充线上尚未填的 4 个字段：
          holiday_name / holiday_day_name / holiday_day_index
          booking_day_name / booking_day_index
      → 保证 16 个字段的名称、顺序、类型、取值格式与线上完全一致，可直接 UNION 或替换 2027 行

规则来源：用户提供 + 2016-2026 历史回测校准
  A. 固定日期大假（元旦/春节/劳动/国庆）—— 从节日当天起算 N 天，不向周末靠
       元旦 N=3、春节 N=8（除夕起）、劳动 N=5、国庆 N=7
       历史命中：国庆 7/7、劳动 5/7、春节起始日 8/10
  B. 农历/节气短假（清明/端午/中秋，N=3）—— 按锚点星期定位，历史 28/28 = 100%
       周一/周二           → 锚点为窗口第 3 天（向前接上周末）
       周三/周四/周五/周六 → 锚点为窗口第 1 天（从锚点当天往后排）
       周日                → 锚点为窗口第 2 天（居中）
     注：周三归入「往后排」由用户 2026-10-14 确认（2027 端午 6/9 周三 → 放假 6/9~6/11）。
         历史 28 例中锚点落周三的有 0 例，故该归并不影响回测命中率（仍 28/28）。

字段取值格式（已核实线上表）：
  holiday_name      = 'X假期'          例：春节假期
  holiday_day_name  = 'X出行第N天'     例：春节出行第1天
  booking_day_name  = 'X开抢第N天'     例：春节开抢第1天
  days_to_cny_eve   = 1-6月距当年除夕(正) / 7-12月距次年除夕(负)，切换点 6/30→7/1

  C. 连休封顶 —— 历史上连休恒等于 N（春节 2019-23 为 7、2024/25 为 8、2026 为 9，从无 10 天），
     机制是把【紧邻放假区间的周末】调休成上班日。本脚本据此自动生成调休日。
     调休天数可用「借天数 = N − 法定天数 − 区间内自然周末天数」核对，10 例中 9 例命中
     （唯一例外 2024 春节：当年除夕不是法定假日，即引发舆论批评的那次）。

⚠️ 待官方公布后核对（见 MANUAL_NOTES）：
  1. 劳动节默认 5/1 起算（历史 2/7 次提前到 4 月底衔接周末）
  2. 调休上班日的落点：本脚本取紧邻周末，历史上也存在非紧邻的还账式调休
     （如 2024 国庆调 9/29+10/12）。调休对出票量预测属噪声，不影响模型。
  3. 调休上班日：线上表无对应字段，单独输出到 dim_date_2027_makeup.csv
"""
from datetime import date, timedelta
import json
import subprocess
import pandas as pd

SCRIPTS = '/Users/xinyhu/.agents/skills/adhoc-artnova-data-access/scripts'
OUT_DIR = '/Users/xinyhu/Desktop/预算预测项目'
WD = ['一', '二', '三', '四', '五', '六', '日']

# 线上表 16 字段（名称与顺序）
COLUMNS = ['date_solar', 'date_lunar', 'day_of_week', 'day_of_month', 'day_of_year',
           'week_of_year', 'firstday_of_week', 'lastday_of_week', 'is_weekend',
           'holiday_name', 'holiday_day_name', 'holiday_day_index',
           'booking_day_name', 'booking_day_index', 'days_to_cny_eve', 'd']
INT_COLS = ['day_of_week', 'day_of_month', 'day_of_year', 'week_of_year', 'is_weekend',
            'holiday_day_index', 'booking_day_index', 'days_to_cny_eve']

# ============ 2027 参数（可调）============
ANCHOR_2027 = {
    '元旦': date(2027, 1, 1),    # 周五
    '除夕': date(2027, 2, 5),    # 周五 —— 春节假期起算点
    '清明': date(2027, 4, 5),    # 周一
    '劳动': date(2027, 5, 1),    # 周六
    '端午': date(2027, 6, 9),    # 周三  ⚠️
    '中秋': date(2027, 9, 15),   # 周三  ⚠️
    '国庆': date(2027, 10, 1),   # 周五
}
# 2028 元旦：其开抢期落在 2027-12，必须一并生成（线上表口径：元旦开抢从上年 12/18 起）
ANCHOR_2028_NY = date(2028, 1, 1)        # 周六
N_DAYS = {'元旦': 3, '春节': 8, '清明': 3, '劳动': 5, '端午': 3, '中秋': 3, '国庆': 7}
FIXED_START = {'元旦', '春节', '劳动', '国庆'}

# 开抢期规则（已用 2024/2025/2026 全部节日验证，无例外）：
#   开抢起 = 放假起 − BOOKING_LEAD 天；开抢止 = 放假止
#   故 开抢天数 = N + BOOKING_LEAD
# ⚠️ 不要用"历史 booking_day_index 最大值"反推——那会混入不同年份的不同 N（我曾因此把
#    春节算成 23 天（实为 N=8 时的 22）、中秋算成 16 天（实为 17））
BOOKING_LEAD = 14

MANUAL_NOTES = {
    '端午': '锚点周三→往后排（用户确认 6/9~6/11）',
    '中秋': '锚点周三→往后排（同端午规则）',
    '春节': 'N=8（用户已拍板）',
    '劳动': '历史 2/7 次提前到 4 月底衔接周末',
}


def plan_lunar(anchor: date, n: int):
    """农历/节气短假定位。历史 28/28 全中。返回 (start, end, makeup_days)

    调休日不在此处决定：统一由 build_holiday_plans() 的「连休封顶」逻辑按
    「紧邻放假区间的周末调休成上班日」生成，与历史做法一致。
    """
    wd = anchor.weekday()
    if wd in (0, 1):                      # 周一/周二 → 锚点第 n 天（向前接上周末）
        s = anchor - timedelta(days=n - 1)
    elif wd in (2, 3, 4, 5):              # 周三~周六 → 锚点第 1 天（从锚点当天往后排）
        s = anchor
    else:                                 # 周日 → 锚点第 2 天（居中）
        s = anchor - timedelta(days=1)
    return s, s + timedelta(days=n - 1), []


def build_holiday_plans():
    plans = {}
    for h, n in N_DAYS.items():
        anc = ANCHOR_2027['除夕'] if h == '春节' else ANCHOR_2027[h]
        if h in FIXED_START:
            s, e, makeup = anc, anc + timedelta(days=n - 1), []
            src = '固定日起算'
        else:
            s, e, makeup = plan_lunar(anc, n)
            src = '向周末靠'
        plans[h] = {'anchor': anc, 'start': s, 'end': e, 'n': n,
                    'src': src, 'makeup': makeup}
    # 2028 元旦（跨年开抢期：其开抢日落在 2027-12）
    plans['元旦_2028'] = {'anchor': ANCHOR_2028_NY, 'start': ANCHOR_2028_NY,
                          'end': ANCHOR_2028_NY + timedelta(days=N_DAYS['元旦'] - 1),
                          'n': N_DAYS['元旦'], 'src': '固定日起算（跨年）', 'makeup': []}

    # 连休封顶：历史上春节连休恒等于 N（2024/25 为 8、2026 为 9，从无 10 天），
    # 机制是把紧邻放假区间的周末调休成上班日。2027 春节 2/5(五)~2/12(五) 后面紧跟
    # 2/13(六)+2/14(日)，不封顶连休会达 10 天，超出全部先例 → 这两天应调休上班。
    for key, p in plans.items():
        extra = []
        nxt = p['end'] + timedelta(days=1)
        while nxt.weekday() >= 5:                 # 向后削掉紧邻周末
            extra.append(nxt)
            nxt += timedelta(days=1)
        prv = p['start'] - timedelta(days=1)
        while prv.weekday() >= 5:                 # 向前削掉紧邻周末
            extra.append(prv)
            prv -= timedelta(days=1)
        p['makeup'] = sorted(set(p['makeup']) | set(extra))
    return plans


def fetch_online_base(year=2027):
    """取线上表该年的行作为底座"""
    sql = (f"SELECT {', '.join(COLUMNS)} FROM dim_traindb.dim_trn_date_label_df "
           "WHERE d=(SELECT MAX(d) FROM dim_traindb.dim_trn_date_label_df) "
           f"AND date_solar BETWEEN '{year}-01-01' AND '{year}-12-31' ORDER BY date_solar")
    r = subprocess.run(['python3', 'adhoc_sql.py', '--browser', 'chrome',
                        '--exec-mode', 'STARROCKS_HIVE', '--insecure',
                        '--sql', sql, '--format', 'json'],
                       cwd=SCRIPTS, capture_output=True, text=True, timeout=240)
    txt = r.stdout
    k = txt.find('{')
    if k < 0:
        raise RuntimeError(f'取数失败: {txt[:400]}')
    d = json.loads(txt[k:txt.rindex('}') + 1])
    if d.get('status') != 'FINISHED':
        raise RuntimeError(f"查询未完成: {str(d)[:400]}")
    df = pd.DataFrame(d['rows'], columns=d['columns'])
    return df.replace('null', None)


def fill_holiday_fields(base: pd.DataFrame, plans: dict):
    """只覆盖 5 个节假日/开抢字段，其余保持线上原值

    开抢期重叠规则（已核对线上 2026-09 实值）：两个节日的开抢期相交时，
    【后开始的接管】，前者被截断。例 2026：中秋开抢 9/11 起本应 17 天，
    但国庆开抢 9/17 起接管，中秋只标到 9/16（6 天）；9/25~9/27 三天
    holiday_name=中秋假期 而 booking_day_name=国庆开抢第9~11天（两字段独立）。
    """
    hol, bk = {}, {}
    # 按开抢期起始日升序写入 → 后开始的自然覆盖先开始的，与线上一致
    ordered = sorted(plans.items(), key=lambda kv: kv[1]['start'])
    for key, p in ordered:
        h = key.split('_')[0]          # '元旦_2028' -> '元旦'，保证字段值不含内部后缀
        for i in range((p['end'] - p['start']).days + 1):
            day = (p['start'] + timedelta(days=i)).isoformat()
            hol[day] = (f'{h}假期', f'{h}出行第{i+1}天', i + 1)
        bk_start = p['start'] - timedelta(days=BOOKING_LEAD)   # 放假起 − 14 天
        span = (p['end'] - bk_start).days + 1                  # = N + 14（未被截断时）
        for i in range(span):
            day = (bk_start + timedelta(days=i)).isoformat()
            bk[day] = (f'{h}开抢第{i+1}天', i + 1)

    df = base.copy()
    df['holiday_name']      = df['date_solar'].map(lambda x: hol[x][0] if x in hol else None)
    df['holiday_day_name']  = df['date_solar'].map(lambda x: hol[x][1] if x in hol else None)
    df['holiday_day_index'] = df['date_solar'].map(lambda x: hol[x][2] if x in hol else None)
    df['booking_day_name']  = df['date_solar'].map(lambda x: bk[x][0] if x in bk else None)
    df['booking_day_index'] = df['date_solar'].map(lambda x: bk[x][1] if x in bk else None)

    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    return df[COLUMNS]


def main():
    plans = build_holiday_plans()

    print('=' * 92)
    print('2027 放假安排（规则生成）')
    print('=' * 92)
    print(f"{'节日':<6}{'锚点':>12}{'周':>4}{'N':>3}  {'放假区间':<32}{'连休':>5}  {'调休上班':<16}备注")
    for h in ['元旦', '春节', '清明', '劳动', '端午', '中秋', '国庆']:
        p = plans[h]
        s, e = p['start'], p['end']
        mk = set(p['makeup'])
        ls, le = s, e
        while (ls - timedelta(days=1)).weekday() >= 5 and (ls - timedelta(days=1)) not in mk:
            ls -= timedelta(days=1)
        while (le + timedelta(days=1)).weekday() >= 5 and (le + timedelta(days=1)) not in mk:
            le += timedelta(days=1)
        mkstr = ', '.join(f'{d.isoformat()}(周{WD[d.weekday()]})' for d in p['makeup']) or '—'
        print(f"{h:<6}{p['anchor'].isoformat():>12}{WD[p['anchor'].weekday()]:>4}{p['n']:>3}  "
              f"{s.isoformat()}({WD[s.weekday()]})~{e.isoformat()}({WD[e.weekday()]}){'':<2}"
              f"{(le - ls).days + 1:>4}天  {mkstr:<16}{MANUAL_NOTES.get(h, '')}")

    print('\n取线上表 2027 行作为底座...')
    base = fetch_online_base(2027)
    print(f'  底座 {len(base)} 行，{len(base.columns)} 字段')

    df = fill_holiday_fields(base, plans)
    out = f'{OUT_DIR}/dim_date_2027_temp.csv'
    df.to_csv(out, index=False, encoding='utf-8-sig')

    # 调休日单独输出（线上表无此字段）
    mk_rows = [{'date_solar': d.isoformat(), 'day_of_week': d.weekday() + 1,
                'makeup_for': h, 'note': '调休上班（线上表无此字段，需单独维护）'}
               for h, p in plans.items() for d in p['makeup']]
    mk_out = f'{OUT_DIR}/dim_date_2027_makeup.csv'
    pd.DataFrame(mk_rows).to_csv(mk_out, index=False, encoding='utf-8-sig')

    print('\n' + '=' * 92)
    print('自检')
    print('=' * 92)
    print(f"  字段顺序一致 {'✓' if list(df.columns) == COLUMNS else '✗'}")
    print(f"  行数 {len(df)}  日期唯一 {'✓' if df['date_solar'].nunique() == len(df) else '✗'}")
    in_2027 = sum(p['n'] for k, p in plans.items() if not k.endswith('_2028'))
    print(f"  holiday_name 非空 {df['holiday_name'].notna().sum()} 天（应为 {in_2027}）")
    print(f"  booking_day_name 非空 {df['booking_day_name'].notna().sum()} 天")
    eve = df[df['date_solar'] == '2027-02-05']['days_to_cny_eve'].iloc[0]
    print(f"  除夕 days_to_cny_eve = {eve} {'✓' if eve == 0 else '✗'}")

    # 格式抽样：与线上历史年份的取值格式对比
    print('\n  取值格式抽样:')
    smp = df[df['holiday_name'].notna()].head(3)
    for _, r in smp.iterrows():
        print(f"    {r['date_solar']}  {r['holiday_name']} / {r['holiday_day_name']}"
              f" / idx={r['holiday_day_index']}")
    smp2 = df[df['booking_day_name'].notna()].head(2)
    for _, r in smp2.iterrows():
        print(f"    {r['date_solar']}  {r['booking_day_name']} / idx={r['booking_day_index']}")

    print(f"\n  已存 主表: {out}")
    print(f"  已存 调休表: {mk_out}（{len(mk_rows)} 天）")
    return df


if __name__ == '__main__':
    main()
