# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import io
import unicodedata as ud

# —— 指定物料号统一 ×1.09（数量不变）——
CODES_FACTOR_109 = {
    "ABB0100493658","ABB0500380618","ABC0194491658","ABL1100210118","ABP0145211648",
    "ABW0441211628","ABW1200210118","ADB0100000018","ADB0100000048","ADB0101211620",
    "ADB0600000018","ADB0600000048","ADB0700000018","ADB0700000048","ADB1600005618",
    "ADB1600362848","ADC0196291720","ADG0800000018","ADG0800000048","ADG2300000048",
    "ADL0261210110","ADL0343362848","ADL0400000018","ADL2000000018","ADL2000000048",
    "ADL2059000418","ADO0700362848","ADS0300000018","ADS0300000048","ADT0100362848",
    "ADV0100000018",
}


def tax_factor_for_code(code: str) -> float:
    """命中名单 → 1.09；否则 1.0"""
    return 1.09 if str(code).strip() in CODES_FACTOR_109 else 1.0

def resolve_sel_month(overview):
    """Return the selected month Period('M') robustly using sel/sel2/overview; never raises NameError."""
    import pandas as pd
    g = globals()
    _ref = None
    # Prefer the 'sel' chosen in 总览
    if "sel" in g:
        try:
            _ref = pd.to_datetime(g["sel"], errors="coerce")
        except Exception:
            _ref = None
    # Fallback to 'sel2' (下钻)
    if (_ref is None) or (pd.isna(_ref)):
        if "sel2" in g:
            try:
                _ref = pd.to_datetime(g["sel2"], errors="coerce")
            except Exception:
                _ref = None
    # Final fallback: newest date in overview
    if (_ref is None) or (pd.isna(_ref)):
        try:
            _ref = pd.to_datetime(overview["日期"], errors="coerce").max()
        except Exception:
            _ref = None
    if _ref is None or pd.isna(_ref):
        return None
    return _ref.to_period("M")

# ===== 常量（同 v5） =====
_SALES_SHEETS    = ["销量-5001","销量-5002","销量"]
_TRANSFER_SHEETS = ["销量-转调理品原料","销量-调拨宫产量","销量-生肉转调理品原料"]
_DATE_CANDS   = ["单据日期","记帐日期","记账日期","凭证日期","日期","输入日期","过账日期"]
_CODE_CANDS   = ["物料","物料号","物料编码","物料编号","物料代码","Material"]
_REV_CANDS    = ["收入-折让 CNY","净收入 CNY","项目货币净值 CNY","收入PN00*n CNY"]
_NETW_CANDS   = ["净重","净重 KG","净重KG","数量(kg)","数量","重量","重量(kg)"]
_EXTAMT_CANDS = ["本币中的外部金额","本位币金额","本位币中的外部金额"]
_QTY_CANDS2   = ["数量","数量(kg)","净重","净重KG","净重 KG","重量","重量(kg)"]

SPECIFIED = [
    "腿类","胸类","胸类-胸","胸类-胸皮","里肌类","翅类","整鸡类","骨架类",
    "爪类","鸡肝类","鸡心类","脖类","鸡胗类","鸡头类","油类"
]
ORDER = SPECIFIED + ["下料类","其他内脏","鸡头+鸡脖+骨架","总计"]
BASE_FOR_TOTAL = [x for x in ORDER if x not in ("胸类","鸡头+鸡脖+骨架","总计")]

def _find_col(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def _pick_col(cols, cands):
    for c in cands:
        if c in cols: return c
    return None

def parse_datecol(series):
    return pd.to_datetime(series, errors="coerce").dt.normalize()

def normalize_code(codes):
    try:
        iterator = list(codes)
    except TypeError:
        iterator = [codes]
    out = []
    for c in iterator:
        if c is None:
            continue
        s = str(c).strip()
        if not s:
            continue
        if '.' in s:
            s = s.split('.')[0]
        out.append(s)
    return out

def _safe_div(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    a = num.to_numpy(dtype=float)
    b = den.to_numpy(dtype=float)
    return np.divide(a, b, out=np.full(a.shape, np.nan, dtype=float), where=(b != 0))

def _attach_rate_display(df, df_lw):
    try:
        if df is None or len(df) == 0:
            return df
        out = df.copy()
        if "日期" not in out.columns or "产量(kg)" not in out.columns:
            return out
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce").dt.normalize()
        lw = df_lw.copy() if df_lw is not None else None
        if isinstance(lw, pd.DataFrame) and not lw.empty:
            cand = ["毛鸡净重(kg)","毛鸡净重","净重","净重(kg)"]
            vcol = next((c for c in cand if c in lw.columns), None)
            dcol = next((c for c in ["日期","交鸡日期","记帐日期","记账日期","凭证日期","过账日期"] if c in lw.columns), None)
            if vcol and dcol:
                lw2 = lw[[dcol, vcol]].copy()
                lw2.columns = ["日期","_lw"]
                lw2["日期"] = pd.to_datetime(lw2["日期"], errors="coerce").dt.normalize()
                lw_day = lw2.groupby("日期", as_index=False)["_lw"].sum()
                out = out.merge(lw_day, on="日期", how="left")
                out["_den"] = out["_lw"].where(out["_lw"].notna() & (out["_lw"]>0), np.nan)
                out["产成率%"] = (out["产量(kg)"] / out["_den"]) * 100.0
                out.drop(columns=[c for c in ["_lw","_den"] if c in out.columns], inplace=True)
        return out
    except Exception:
        return df

# ===== 读取全部 Sheet =====
@st.cache_resource(show_spinner=False)
def read_all_sheets(uploaded):
    if uploaded is None: return None
    raw = uploaded.getvalue()
    try: return pd.ExcelFile(io.BytesIO(raw))
    except Exception: return None

# ===== 读取产量（按日按物料号） =====
def read_qty_per_code_per_day(xls):
    if xls is None or ("产量" not in xls.sheet_names):
        return pd.DataFrame(columns=["日期","物料号","产量(kg)"])
    df = pd.read_excel(xls, "产量")
    df.columns = [str(c).strip() for c in df.columns]
    dcol = _pick_col(df.columns, ["日期","记帐日期","记账日期","凭证日期","过账日期","单据日期"])
    ccol = _pick_col(df.columns, _CODE_CANDS)
    qcol = _pick_col(df.columns, ["数量(kg)","数量","净重","净重KG","净重 KG","重量","重量(kg)","KG","kg"])
    if not (dcol and ccol and qcol):
        return pd.DataFrame(columns=["日期","物料号","产量(kg)"])
    out = pd.DataFrame({
        "日期": pd.to_datetime(df[dcol], errors="coerce").dt.normalize(),
        "物料号": df[ccol].astype(str).str.strip(),
        "产量(kg)": pd.to_numeric(df[qcol], errors="coerce")
    }).dropna(subset=["日期","物料号","产量(kg)"])
    return out.groupby(["日期","物料号"], as_index=False)["产量(kg)"].sum()

# ===== BOM 映射（含胸类细分与别名宽匹配） =====
@st.cache_data(show_spinner=False)
def read_bom_mapping(uploaded):
    code2major = {}
    if uploaded is None: return {}
    raw = uploaded.getvalue()
    try: xls = pd.ExcelFile(io.BytesIO(raw))
    except Exception: return {}
    def canon_major(text: str) -> str:
        if not isinstance(text, str): return ""
        t = text.strip().replace(" ", "")
        if "肉架" in t or "鸡架" in t: return "肉架类"   # ← 新增识别
        if "胸" in t: return "胸类"
        if "腿" in t: return "腿类"
        if ("里肌" in t) or ("里脊" in t) or ("里脇" in t): return "里肌类"
        if "翅" in t: return "翅类"
        if "整鸡" in t: return "整鸡类"
        if "骨架" in t: return "骨架类"
        if ("鸡爪" in t) or (t.endswith("爪")) or ("爪" in t): return "爪类"
        if ("鸡肝" in t) or ("肝" in t and "鸡" in t): return "鸡肝类"
        if ("鸡心" in t) or ("心" in t and "鸡" in t): return "鸡心类"
        if ("脖" in t) or ("鸡脖" in t): return "脖类"
        if ("鸡头" in t) or ("头" in t and "鸡" in t): return "鸡头类"
        if ("鸡胗" in t) or ("鸡肫" in t) or (t in ["胗","胗类"]) or ("胗" in t): return "鸡胗类"
        if "油" in t: return "油类"
        if "下料" in t: return "下料类"
        if "内脏" in t: return "其他内脏"
        return t
    def _parse(df):
        cols = [str(c).strip() for c in df.columns]
        code_col = next((c for c in cols if any(k in c for k in ["物料号","物料编码","物料编号","物料代码","编码","代码","Material","物料"])), None)
        maj_col  = next((c for c in cols if any(k in c for k in ["外部物料组描述","物料组描述","部位","部位大类","类别","大类","分类"])), None)
        sub_col  = next((c for c in cols if any(k in c for k in ["子类","二级","小类","品类","部位描述"])), None)
        if (code_col is None) or (maj_col is None): return
        tmp = df[[code_col, maj_col] + ([sub_col] if sub_col else [])].dropna(subset=[code_col, maj_col]).copy()
        tmp[code_col] = normalize_code(tmp[code_col])
        tmp[maj_col]  = tmp[maj_col].astype(str).map(canon_major)
        for _, r in tmp.iterrows():
            code = str(r[code_col]).strip(); maj = str(r[maj_col]).strip()
            if not code or not maj: continue
            final_major = "胸类-胸皮" if (maj=="胸类" and ("胸皮" in str(r.get(sub_col,"")).replace(" ", ""))) else ("胸类-胸" if maj=="胸类" else maj)
            code2major[code] = final_major
    try:
        for s in xls.sheet_names:
            _parse(pd.read_excel(xls, s))
    except Exception: pass
    # 强制归类（你的固定清单）
    def _force_put(codes, major):
        for v in normalize_code(codes):
            code2major[v] = major
    _force_put(["ABF0100322058","ABF0100493650","ABF0600493638","ADF0100320110","ADF0100323640","ADF0600493600"], "油类")
    _force_put(["ABO0900945618","ABZ0600493658"], "下料类")
    # ↓ 替换原来的胸皮强制映射这一行 ↓
    _force_put([
        "ABB0700210118",
        "ABB0700380618",
        "ABB0700493610",
        "ABB0700493638",
        "ADB0700000008",
        "ADB0700000018",
        "ADB0700000048",
        "ADB0700000058",
        "ADB0700380610",
    ], "胸类-胸皮")
    # ↑ 只替换这一段即可，其他大类的 _force_put 不变 ↑

    _force_put(["ABO0100493640","ABO0100493658","ABO0300210118","ABO0300211658","ABO0300493658",
                "ABO0800493648","ABZ0400723608","ADO0100210100","ADO0100380600","ADO0300210100",
                "ADO0300380600","ADO1500945610"], "其他内脏")
    # 如需强制映射具体“肉架/鸡架”物料号，可在此追加：
    # _force_put(["XXXXXX","YYYYYY"], "肉架类")
    return code2major

# ===== 构建“原始”当日代码单价（销量 & 转调理/调拨宫） =====
def build_daily_code_price_raw(xls):
    """
    综合单价（当日代码价）生成口径（更新：销售 与 转调/调拨 同一优先级，可合并加权）：
    - 若当日该物料在 销售(5001/5002) 或 转调理品原料/调拨宫产量 中任一来源出现：
        金额 = 销售金额 + 转调金额_含税（两者按列求和，缺失当 0）
        数量 = 销售净重 + 转调数量
        综合单价 = 金额 / 数量   （数量允许为负；仅数量==0 或 NaN 时置 NaN）
    - 若两类来源都没有记录：综合单价 = NaN（留给 fill_price_code_month_avg 回填）
    """
    try:
        sheets = xls.sheet_names
    except Exception:
        return pd.DataFrame(columns=["日期","物料号","综合单价","金额","数量"])

    # --- 分别采集 销售 与 转调/调拨 两类来源 ---
    sale_frames = []
    trans_frames = []

    for sh in sheets:
        df = pd.read_excel(xls, sh)
        df.columns = [str(c).strip() for c in df.columns]

        # 公共列定位
        dcol = _pick_col(df.columns, _DATE_CANDS)
        ccol = _pick_col(df.columns, _CODE_CANDS)
        if not (dcol and ccol):
            continue

        if sh in _SALES_SHEETS:
            rev = _pick_col(df.columns, _REV_CANDS)   # 收入-折让 CNY（或同义列）
            wt  = _pick_col(df.columns, _NETW_CANDS)  # 净重/数量(kg)
            if not (rev and wt):
                continue
            tmp = pd.DataFrame({
                "日期": pd.to_datetime(df[dcol], errors="coerce").dt.normalize(),
                "物料号": df[ccol].astype(str).str.strip(),
                "销售金额": pd.to_numeric(df[rev], errors="coerce"),
                "销售净重": pd.to_numeric(df[wt], errors="coerce")
            }).dropna(subset=["日期","物料号"])
            if not tmp.empty:
                sale_frames.append(tmp.groupby(["日期","物料号"], as_index=False)[["销售金额","销售净重"]].sum())

        elif sh in _TRANSFER_SHEETS:
            ext = _pick_col(df.columns, _EXTAMT_CANDS)  # 本币中的外部金额/本位币金额
            qty = _pick_col(df.columns, _QTY_CANDS2)    # 数量(kg)/净重
            if not (ext and qty):
                continue
            tmp = pd.DataFrame({
                "日期": pd.to_datetime(df[dcol], errors="coerce").dt.normalize(),
                "物料号": df[ccol].astype(str).str.strip(),
                "转调金额_含税": pd.to_numeric(df[ext], errors="coerce"),
                "转调数量": pd.to_numeric(df[qty], errors="coerce")
            }).dropna(subset=["日期","物料号"])
            if not tmp.empty:
                trans_frames.append(tmp.groupby(["日期","物料号"], as_index=False)[["转调金额_含税","转调数量"]].sum())

    # 合并销售与转调/调拨
    if not sale_frames and not trans_frames:
        return pd.DataFrame(columns=["日期","物料号","综合单价","金额","数量"])

    sale  = pd.concat(sale_frames,  ignore_index=True) if sale_frames  else pd.DataFrame(columns=["日期","物料号","销售金额","销售净重"])
    trans = pd.concat(trans_frames, ignore_index=True) if trans_frames else pd.DataFrame(columns=["日期","物料号","转调金额_含税","转调数量"])

    comb = sale.merge(trans, on=["日期","物料号"], how="outer")

    # 统一优先级：能加则加；两边都空则 NaN
    has_any = comb[["销售金额","销售净重","转调金额_含税","转调数量"]].notna().any(axis=1)

    comb["金额"] = np.where(
        has_any,
        comb[["销售金额","转调金额_含税"]].sum(axis=1, skipna=True),
        np.nan
    )
    comb["数量"] = np.where(
        has_any,
        comb[["销售净重","转调数量"]].sum(axis=1, skipna=True),
        np.nan
    )

    comb = comb[["日期", "物料号", "金额", "数量"]]
    comb = comb.groupby(["日期", "物料号"], as_index=False)[["金额", "数量"]].sum(min_count=1)
    # —— 对指定物料号把“金额”统一×1.09（数量不变）——
    if 'tax_factor_for_code' in globals():
        comb["金额"] = comb["金额"] * comb["物料号"].apply(tax_factor_for_code)

    # 数量允许为负；仅数量==0/NaN 时置 NaN
    comb["综合单价"] = np.where(comb["数量"] != 0, comb["金额"] / comb["数量"], np.nan)

    # 数量允许为负；仅数量==0/NaN 时置 NaN 避免除零
    comb["综合单价"] = np.where(comb["数量"] != 0, comb["金额"] / comb["数量"], np.nan)

    return comb

def fill_price_code_month_avg(pr_raw, qty, code2major, manual_month=None):
    """
    用于价格断档回填：
    - 当日该物料没有当日价时，用“本月1号~前一日”的**加权平均价**（按数量加权）回填其综合单价；
      加权口径：到“前一日”为止的 累计金额 / 累计数量（仅统计数量>0的当日记录）。
    - 若该物料在当月前一日之前也完全没有价，则按大类当日加权价 → 大类近7天滚动加权价 → 大类期内中位数 兜底。
    """
    import pandas as _pd
    import numpy as _np

    if pr_raw is None or pr_raw.empty or qty is None or qty.empty:
        return _pd.DataFrame(columns=["日期","物料号","综合单价_filled"])

    # 标准化
    pr = pr_raw.copy()
    pr["日期"] = _pd.to_datetime(pr["日期"], errors="coerce").dt.normalize()
    qty2 = qty.copy()
    qty2["日期"] = _pd.to_datetime(qty2["日期"], errors="coerce").dt.normalize()

    # 需要产出价格的 (日期, 物料号)
    need = qty2[["日期","物料号"]].drop_duplicates()
    need["月"] = need["日期"].dt.to_period("M")

    # 当日已有价（综合单价）
    pr["月"] = pr["日期"].dt.to_period("M")
    pr_day = pr[["日期","物料号","月","综合单价","金额","数量"]].copy()

    # 先左连当日实价
    out0 = need.merge(pr_day[["日期","物料号","月","综合单价"]], on=["日期","物料号","月"], how="left")

    # === 本月1号~前一日的“加权平均价”回填 ===
    code_filled = []
    agg = (pr_day.groupby(["物料号","月","日期"], as_index=False)
                 .agg(金额=("金额","sum"), 数量=("数量","sum")))

    # 数值清洗 + 安全过滤
    try:
        _col_qty = (agg["数量"].astype(str)
                              .str.replace(",", "", regex=False)
                              .str.replace("—", "", regex=False)
                              .str.strip())
    except Exception:
        _col_qty = agg["数量"]
    _qty_vals = _pd.to_numeric(_col_qty, errors="coerce").to_numpy(dtype=float)
    _qty_mask = _np.isfinite(_qty_vals) & (_qty_vals != 0)
    agg = agg.loc[_qty_mask].copy()
    agg["数量"] = _qty_vals[_qty_mask]

    for (code, month), gneed in out0[out0["综合单价"].isna()].groupby(["物料号","月"]):
        g = agg[(agg["物料号"]==code) & (agg["月"]==month)].sort_values("日期").copy()
        if g.empty:
            tmp = gneed.copy()
            tmp["综合单价_filled"] = _np.nan
            code_filled.append(tmp[["日期","物料号","综合单价_filled"]])
            continue

        g["cum_amt"] = g["金额"].cumsum()
        g["cum_qty"] = g["数量"].cumsum()
        g["mtd_wavg"] = _np.where(g["cum_qty"] != 0, g["cum_amt"]/g["cum_qty"], _np.nan)

        gneed_sorted  = gneed.sort_values("日期")
        gprice_sorted = g[["日期","mtd_wavg"]].sort_values("日期")

        filled = _pd.merge_asof(
            gneed_sorted,
            gprice_sorted,
            left_on="日期",
            right_on="日期",
            direction="backward",
            allow_exact_matches=False
        )
        filled.rename(columns={"mtd_wavg":"综合单价_filled"}, inplace=True)
        code_filled.append(filled[["日期","物料号","综合单价_filled"]])

    code_filled = _pd.concat(code_filled, ignore_index=True) if code_filled else _pd.DataFrame(columns=["日期","物料号","综合单价_filled"])

    # 优先用当日价，其次用加权均价回填
    out = out0.merge(code_filled, on=["日期","物料号"], how="left")
    out["综合单价_filled"] = out["综合单价"].where(out["综合单价"].notna(), out["综合单价_filled"])

    # === 手工补价 ===
    if manual_month is not None and not getattr(manual_month, 'empty', True):
        mm = manual_month.copy()
        mm['物料号'] = mm['物料号'].astype(str).str.strip()
        out = out.merge(mm.rename(columns={'手工单价':'_手工单价'}), on='物料号', how='left')
        out['综合单价_filled'] = out['综合单价_filled'].where(out['综合单价_filled'].notna(), out['_手工单价'])
        if '_手工单价' in out.columns:
            out.drop(columns=['_手工单价'], inplace=True)

    # === 大类兜底（保持原口径） ===
    pr_raw2 = pr_raw.copy()
    pr_raw2["日期"] = _pd.to_datetime(pr_raw2["日期"], errors="coerce").dt.normalize()
    pr_raw2["部位"] = pr_raw2["物料号"].map(code2major)
    pr_raw2 = pr_raw2[pr_raw2["部位"].notna()]

    cat_day = pr_raw2.groupby(["日期","部位"], as_index=False)[["金额","数量"]].sum()
    cat_day["大类价"] = _np.where(cat_day["数量"] != 0, cat_day["金额"]/cat_day["数量"], _np.nan)

    cat_day = cat_day.sort_values(["部位","日期"])
    cat_day["大类价_7d"] = _np.nan
    for b, g in cat_day.groupby("部位"):
        s = g.set_index("日期")["大类价"].rolling("7D", min_periods=1).mean()
        cat_day.loc[g.index, "大类价_7d"] = s.values

    cat_med = cat_day.groupby("部位", as_index=False)["大类价"].median().rename(columns={"大类价":"大类价_中位"})

    out["部位"] = out["物料号"].map(code2major)
    out = out.merge(cat_day[["日期","部位","大类价","大类价_7d"]], on=["日期","部位"], how="left")
    out = out.merge(cat_med, on="部位", how="left")

    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价"])
    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价_7d"])
    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价_中位"])

    return out[["日期","物料号","综合单价_filled"]]

def read_part_allocation(uploaded):
    """
    输入：分摊表（Excel/CSV）。推荐列：日期(可选)、物料号(必填)、部位列(若干)、合计(可选)；部位列可为百分比或0~1。
    输出：长表：['日期','物料号','项目','权重']；其中“项目”为模型的大类名。
    匹配优先级：按日(日期+物料号) > 通用(仅物料号)。
    规则：填写“日期”→仅该日生效；未填写→默认通用（当月所有日期生效）。
    """
    import pandas as pd, numpy as np, io as _io

    if uploaded is None:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    raw = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
    try:
        if (getattr(uploaded, "type", "") or "").endswith("csv") or str(getattr(uploaded, "name","")).lower().endswith(".csv"):
            df = pd.read_csv(_io.BytesIO(raw))
        else:
            df = pd.read_excel(_io.BytesIO(raw), sheet_name=0)
    except Exception:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    df.columns = [str(c).strip() for c in df.columns]

    # 键列
    dcol = next((c for c in ["日期","单据日期","记帐日期","记账日期","凭证日期","过账日期"] if c in df.columns), None)
    ccol = next((c for c in ["物料号","物料","物料编码","物料编号","物料代码","Material"] if c in df.columns), None)
    if not ccol:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    # 识别分摊列
    exclude = {dcol, ccol, "公司","工厂","物料描述","品类","序号","部位","合计", None}
    part_cols = [c for c in df.columns if c not in exclude]

    part2major = {
        "全腿":"腿类", "腿":"腿类",
        "胸肉块":"胸类-胸","胸":"胸类-胸","胸类-胸":"胸类-胸",
        "带筋里":"里肌类","里肌":"里肌类",
        "全翅":"翅类","翅":"翅类",
        "爪":"爪类","鸡爪":"爪类",
        "鸡肝":"鸡肝类","肝":"鸡肝类",
        "鸡心":"鸡心类","心":"鸡心类",
        "去皮脖":"脖类","脖":"脖类","鸡脖":"脖类",
        "鸡头":"鸡头类","头":"鸡头类",
        "半架":"骨架类","鸡骨架":"骨架类","腿骨泥":"骨架类","腿骨":"骨架类","骨架":"骨架类",
        "胃":"鸡胗类","鸡胗":"鸡胗类","胗":"鸡胗类",
        "下料":"下料类","地脚料":"下料类","下料类":"下料类",
        "其他内脏":"其他内脏",
        "肉架":"肉架类", "鸡架":"肉架类"   # ← 新增：分摊表可直接用“肉架/鸡架”列
    }

    keep_pairs = []
    for c in part_cols:
        std = part2major.get(c, c)
        keep_pairs.append((c, std))
    if not keep_pairs:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    def _safe_day(x):
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip()==""):
            return pd.NaT
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            try:
                return (pd.Timestamp("1899-12-30") + pd.to_timedelta(float(x), unit="D")).normalize()
            except Exception:
                pass
        t = pd.to_datetime(x, errors="coerce")
        return t.normalize() if pd.notna(t) else pd.NaT

    rows = []
    for _, r in df.iterrows():
        code = str(r[ccol]).strip() if ccol in df.columns else ""
        if not code or code.lower()=="nan":
            continue
        day = _safe_day(r.get(dcol, None)) if dcol else pd.NaT

        weights = []
        for raw_name, std_name in keep_pairs:
            val = r.get(raw_name, None)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            s = str(val).strip()
            if not s:
                continue
            try:
                v = float(s.replace("%",""))
                if ("%" in s) or v>1.001:
                    v = v/100.0
            except Exception:
                continue
            if v>0:
                weights.append((std_name, v))

        if not weights:
            continue
        tot = sum(v for _, v in weights)
        if tot<=0:
            continue
        weights = [(name, v/tot) for name, v in weights]

        for name, v in weights:
            rows.append({"日期": day, "物料号": code, "项目": name, "权重": float(v)})

    out = pd.DataFrame(rows, columns=["日期","物料号","项目","权重"])
    if not out.empty:
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
    return out

def apply_part_allocation(m_code_daily, alloc_long, code2major):
    """
    有‘日期’→只在该日生效；‘日期’空→默认通用（整月）。
    层级：按日 > 默认(通用) > 一对一映射。
    修复点：通用规则合并时去掉分摊表的“日期”列，避免合并后出现 日期_x/日期_y 导致 KeyError。
    """
    import pandas as pd, numpy as np
    if m_code_daily is None or m_code_daily.empty:
        return pd.DataFrame(columns=["日期","项目","物料号","产量(kg)","含税金额"])

    base = m_code_daily.copy()
    base["含税金额"] = base["产量(kg)"] * base["综合单价_filled"]

    if alloc_long is None or alloc_long.empty:
        base["项目"] = base["物料号"].map(code2major)
        return base[["日期","项目","物料号","产量(kg)","含税金额"]]

    day_alloc = alloc_long[alloc_long["日期"].notna()].copy()
    mon_alloc = alloc_long[alloc_long["日期"].isna()].copy()

    parts = []

    # 1) 按日分摊（精确匹配 日期+物料号）
    if not day_alloc.empty:
        a = base.merge(day_alloc, on=["日期","物料号"], how="inner")
        if not a.empty:
            a["产量(kg)"] = a["产量(kg)"] * a["权重"]
            a["含税金额"] = a["含税金额"] * a["权重"]
            parts.append(a[["日期","项目","物料号","产量(kg)","含税金额"]])

    # 2) 默认通用（对未覆盖的行，按 物料号 套用）
    if not mon_alloc.empty:
        mon_alloc_nodate = mon_alloc.drop(columns=["日期"], errors="ignore").copy()

        if parts:
            used = pd.concat(parts, ignore_index=True)[["日期","物料号"]].drop_duplicates()
            rem = base.merge(used, on=["日期","物料号"], how="left", indicator=True)
            rem = rem[rem["_merge"]=="left_only"].drop(columns="_merge")
        else:
            rem = base

        b = rem.merge(mon_alloc_nodate, on=["物料号"], how="inner")

        if "日期_x" in b.columns and "日期" not in b.columns:
            b = b.rename(columns={"日期_x":"日期"})
        if "日期_y" in b.columns:
            b = b.drop(columns=["日期_y"])

        if not b.empty:
            b["产量(kg)"] = b["产量(kg)"] * b["权重"]
            b["含税金额"] = b["含税金额"] * b["权重"]
            parts.append(b[["日期","项目","物料号","产量(kg)","含税金额"]])

    # 3) 仍未命中 → 回落到一对一映射
    if parts:
        covered = pd.concat(parts, ignore_index=True)[["日期","物料号"]].drop_duplicates()
        rest = base.merge(covered, on=["日期","物料号"], how="left", indicator=True)
        rest = rest[rest["_merge"]=="left_only"].drop(columns="_merge")
    else:
        rest = base

    if not rest.empty:
        rest["项目"] = rest["物料号"].map(code2major)
        parts.append(rest[["日期","项目","物料号","产量(kg)","含税金额"]])

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["日期","项目","物料号","产量(kg)","含税金额"]
    )

# ===== 读取净重台账（与 v5 相同） =====
@st.cache_data(show_spinner=False)
def read_liveweight(uploaded):
    if uploaded is None: return pd.DataFrame(columns=["日期","毛鸡净重(kg)"])
    raw = uploaded.getvalue()
    try:
        if getattr(uploaded,"type","").endswith("csv") or uploaded.name.lower().endswith(".csv"):
            df=pd.read_csv(io.BytesIO(raw))
            dcol=_find_col(df.columns,["日期","交鸡日期","记帐日期","记账日期","凭证日期","过账日期"])
            vcol=_find_col(df.columns,["毛鸡净重(kg)","毛鸡净重","净重","净重(kg)"])
            if dcol and vcol:
                out=pd.DataFrame({"日期":parse_datecol(df[dcol]),"毛鸡净重(kg)":pd.to_numeric(df[vcol],errors="coerce")})
                out=out[out["日期"].notna() & out["毛鸡净重(kg)"].notna()]
                if not out.empty: return out.groupby("日期",as_index=False)["毛鸡净重(kg)"].sum()
        else:
            xls=pd.ExcelFile(io.BytesIO(raw)); parts=[]
            for s in xls.sheet_names:
                df=pd.read_excel(xls,s); df.columns=[str(c).strip() for c in df.columns]
                dcol=_find_col(df.columns,["日期","交鸡日期","记帐日期","记账日期","凭证日期","过账日期"])
                vcol=_find_col(df.columns,["毛鸡净重(kg)","毛鸡净重","净重","净重(kg)"])
                if dcol and vcol:
                    out=pd.DataFrame({"日期":parse_datecol(df[dcol]),"毛鸡净重(kg)":pd.to_numeric(df[vcol],errors="coerce")})
                    out=out[out["日期"].notna() & out["毛鸡净重(kg)"].notna()]
                    if not out.empty: parts.append(out)
            if parts:
                full=pd.concat(parts,ignore_index=True)
                return full.groupby("日期",as_index=False)["毛鸡净重(kg)"].sum()
    except Exception: pass
    return pd.DataFrame(columns=["日期","毛鸡净重(kg)"])

# ===== 读取补价表（按月，物料号→含税单价） =====
@st.cache_data(show_spinner=False)
def read_manual_month_price(uploaded):
    """
    读取手工补价表（Excel 或 CSV）。支持列名同义：
      - 物料列：物料号/物料编码/物料
      - 价格列：含税单价/综合单价/单价/售价/价格/PRICE
    """
    import pandas as _pd
    if uploaded is None:
        return _pd.DataFrame(columns=['物料号','手工单价'])
    try:
        raw = uploaded.getvalue()
        if getattr(uploaded, "type", "").endswith("csv") or uploaded.name.lower().endswith(".csv"):
            df = _pd.read_csv(io.BytesIO(raw))
        else:
            df = _pd.read_excel(io.BytesIO(raw))
        df.columns = [str(c).strip() for c in df.columns]
        ccol = next((c for c in ['物料号','物料编码','物料','Material'] if c in df.columns), None)
        pcol = next((c for c in ['含税单价','综合单价','单价','售价','价格','PRICE','price'] if c in df.columns), None)
        if not (ccol and pcol):
            return _pd.DataFrame(columns=['物料号','手工单价'])
        out = _pd.DataFrame({
            '物料号': df[ccol].astype(str).str.strip(),
            '手工单价': _pd.to_numeric(df[pcol], errors='coerce')
        })
        out = out.dropna(subset=['物料号','手工单价'])
        out = out[out['物料号']!='']
        out = out.groupby('物料号', as_index=False)['手工单价'].mean()
        return out
    except Exception:
        return _pd.DataFrame(columns=['物料号','手工单价'])

def build_overview(xls, code2major, df_lw=None, manual_month_df=None, alloc_long=None):
    # 产量
    qty = read_qty_per_code_per_day(xls)
    if qty.empty: return pd.DataFrame(), pd.DataFrame()
    # 销售价原始
    pr_raw = build_daily_code_price_raw(xls)
    # 价格断档填充：本月截至前一日的平均价
    pr_fill = fill_price_code_month_avg(pr_raw, qty, code2major, manual_month_df)

    m = qty.merge(pr_fill, on=["日期","物料号"], how="left")

    # << 部位分摊：若命中分摊规则则按比例拆分，否则回落到一对一映射 >>
    m_split = apply_part_allocation(m, alloc_long, code2major)

    over = m_split.groupby(["日期","项目"], as_index=False).agg({"产量(kg)":"sum","含税金额":"sum"})
    over["含税单价"] = _safe_div(over["含税金额"], over["产量(kg)"])
    over["项目"] = pd.Categorical(over["项目"], categories=ORDER, ordered=True)
    over = over.sort_values("项目")

    # 胸类（汇总）与组合行
    frames=[]
    for d,g in over.groupby("日期"):
        g2=g.copy(); add=[]
        chest=g2[g2["项目"].isin(["胸类-胸","胸类-胸皮"])]
        if not chest.empty:
            s=chest[["产量(kg)","含税金额"]].sum(); qtyc=s["产量(kg)"]; amtc=s["含税金额"]
            add.append({"日期":d,"项目":"胸类","产量(kg)":qtyc,"含税金额":amtc,"含税单价":(amtc/qtyc if qtyc>0 else 0)})
        comb=g2[g2["项目"].isin(["鸡头类","脖类","骨架类"])]
        if not comb.empty:
            s=comb[["产量(kg)","含税金额"]].sum(); q=s["产量(kg)"]; a=s["含税金额"]
            add.append({"日期":d,"项目":"鸡头+鸡脖+骨架","产量(kg)":q,"含税金额":a,"含税单价":(a/q if q>0 else 0)})
        if add:
            g2=pd.concat([g2,pd.DataFrame(add)],ignore_index=True)
        frames.append(g2)
    over=pd.concat(frames,ignore_index=True)

    # 子类（分摊后下钻）：按“部位大类 + 物料号”
    minors = m_split.rename(columns={"项目":"部位大类","物料号":"子类"})
    minors = minors.groupby(["日期","部位大类","子类"],as_index=False).agg({"产量(kg)":"sum","含税金额":"sum"})
    minors["含税单价"] = _safe_div(minors["含税金额"], minors["产量(kg)"])
    return over, minors

# ===== UI =====
st.sidebar.header("上传数据")
main_excel = st.sidebar.file_uploader("① 主数据", type=["xlsx","xls"])
lw_file   = st.sidebar.file_uploader("② 净重", type=["xlsx","xls","csv"])
bom_map   = st.sidebar.file_uploader("③ 物料清单", type=["xlsx","xls"])
manual_price = st.sidebar.file_uploader("④ 补价表", type=["xlsx","xls","csv"])
alloc_file  = st.sidebar.file_uploader("⑤ 部位分摊表", type=["xlsx","xls","csv"])

if not (main_excel and lw_file and bom_map):
    st.info("请依次上传")
    st.stop()

xls        = read_all_sheets(main_excel)
manual_month_df = read_manual_month_price(manual_price)
df_lw      = read_liveweight(lw_file)
code2major = read_bom_mapping(bom_map)

alloc_long = read_part_allocation(alloc_file)

overview, minors = build_overview(xls, code2major, df_lw, manual_month_df, alloc_long)

st.subheader("总览")
if overview is None or overview.empty:
    st.write("无数据")
else:
    days = sorted(pd.to_datetime(overview["日期"].dropna().unique()))
    sel  = st.selectbox("选择日期", days, index=len(days)-1 if days else 0, format_func=lambda d: pd.to_datetime(d).strftime("%Y-%m-%d"))
    ov = overview[overview["日期"]==sel].copy()

    must = pd.DataFrame({"日期": sel, "项目": SPECIFIED})
    ov = must.merge(ov, on=["日期","项目"], how="left")
    for c in ["产量(kg)","产成率%","含税金额","含税单价"]:
        if c not in ov.columns: ov[c]=np.nan
    ov["产量(kg)"]=ov["产量(kg)"].fillna(0.0)
    ov["含税金额"] = pd.to_numeric(ov["含税金额"], errors="coerce").fillna(0.0)
    ov["含税单价"]=ov["含税单价"].fillna(0.0)

    others = overview[(overview["日期"]==sel) & (~overview["项目"].isin(SPECIFIED))].copy()
    try:
        others["项目"]=pd.Categorical(others["项目"],categories=[x for x in ORDER if x not in SPECIFIED],ordered=True)
        others=others.sort_values("项目")
    except Exception: pass

    # 总计（只汇总基础大类）
    base_today = pd.concat([ov, others], ignore_index=True)
    dynamic_base = set(BASE_FOR_TOTAL)
    present = set(base_today['项目'].dropna().astype(str).unique())
    if '胸类' in present:
        dynamic_base.discard('胸类-胸')
        dynamic_base.discard('胸类-胸皮')
        dynamic_base.add('胸类')
    else:
        dynamic_base.discard('胸类')
    base_today_no_combo = base_today[base_today['项目'].isin(dynamic_base)]
    tot_qty = base_today_no_combo['产量(kg)'].sum(min_count=1)
    tot_amt = base_today_no_combo['含税金额'].sum(min_count=1)
    tot_unit = (tot_amt/tot_qty) if (pd.notna(tot_qty) and tot_qty>0) else 0.0

    total_row = pd.DataFrame({
        "日期":[sel],
        "项目":["总计"],
        "产量(kg)":[float(tot_qty) if pd.notna(tot_qty) else 0.0],
        "含税金额":[float(tot_amt) if pd.notna(tot_amt) else 0.0],
        "含税单价":[float(tot_unit)],
        "产成率%":[np.nan]
    })
    final_ov = pd.concat([base_today, total_row], ignore_index=True)
    disp = _attach_rate_display(final_ov, df_lw).copy()
    for _c in ['产量(kg)','产成率%','含税金额','含税单价']:
        if _c in disp.columns:
            disp[_c] = pd.to_numeric(disp[_c], errors="coerce").round(2)
    if "日期" in disp.columns:
        disp["日期"] = pd.to_datetime(disp["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    _order_cols = ["日期", "项目", "产量(kg)", "产成率%", "含税金额", "含税单价"]
    disp = disp[[c for c in _order_cols if c in disp.columns]]
    if "产成率%" in disp.columns:
        disp["产成率%"] = disp["产成率%"].apply(lambda v: "" if pd.isna(v) else f"{float(v):.2f}%")
    st.dataframe(disp, use_container_width=True)

    # === 累计（本月至所选日） ===
    with st.expander("累计（本月至所选日）", expanded=True):
        sel_dt = pd.to_datetime(sel).normalize()
        month_start = sel_dt.replace(day=1)
        rng = (overview["日期"] >= month_start) & (overview["日期"] <= sel_dt)
        cum = overview.loc[rng].groupby("项目", as_index=False).agg({"产量(kg)":"sum","含税金额":"sum"})
        cum["含税单价"] = np.where(cum["产量(kg)"] != 0, cum["含税金额"]/cum["产量(kg)"], np.nan)

        must_c = pd.DataFrame({"项目": SPECIFIED})
        cum = must_c.merge(cum, on="项目", how="left")
        for c in ["产量(kg)","含税金额","含税单价"]:
            if c not in cum.columns:
                cum[c] = np.nan
        cum["产量(kg)"] = pd.to_numeric(cum["产量(kg)"], errors="coerce").fillna(0.0)
        cum["含税金额"] = pd.to_numeric(cum["含税金额"], errors="coerce").fillna(0.0)
        cum["含税单价"] = pd.to_numeric(cum["含税单价"], errors="coerce").fillna(0.0)

        others_c = (overview.loc[rng & (~overview["项目"].isin(SPECIFIED))]
                            .groupby("项目", as_index=False)
                            .agg({"产量(kg)":"sum","含税金额":"sum"}))
        if not others_c.empty:
            others_c["含税单价"] = np.where(others_c["产量(kg)"] != 0, others_c["含税金额"]/others_c["产量(kg)"], np.nan)
            try:
                others_c["项目"] = pd.Categorical(others_c["项目"], categories=[x for x in ORDER if x not in SPECIFIED], ordered=True)
                others_c = others_c.sort_values("项目")
            except Exception:
                pass

        cum_base = pd.concat([cum, others_c], ignore_index=True) if 'others_c' in locals() else cum

        dynamic_base_cum = set(BASE_FOR_TOTAL)
        present_cum = set(cum_base['项目'].dropna().astype(str).unique())
        if '胸类' in present_cum:
            dynamic_base_cum.discard('胸类-胸')
            dynamic_base_cum.discard('胸类-胸皮')
            dynamic_base_cum.add('胸类')
        else:
            dynamic_base_cum.discard('胸类')
        base_mask = cum_base['项目'].isin(dynamic_base_cum)
        tot_qty = cum_base.loc[base_mask, '产量(kg)'].sum(min_count=1)
        tot_amt = cum_base.loc[base_mask, '含税金额'].sum(min_count=1)
        tot_unit = (tot_amt/tot_qty) if (pd.notna(tot_qty) and tot_qty>0) else 0.0

        total_row = pd.DataFrame({
            "项目":["总计"],
            "产量(kg)":[float(tot_qty) if pd.notna(tot_qty) else 0.0],
            "含税金额":[float(tot_amt) if pd.notna(tot_amt) else 0.0],
            "含税单价":[float(tot_unit)]
        })
        cum_final = pd.concat([cum_base, total_row], ignore_index=True)

        try:
            lw_sum = None
            if df_lw is not None and not df_lw.empty:
                dcol = next((c for c in ["日期","交鸡日期","记帐日期","记账日期","凭证日期","过账日期"] if c in df_lw.columns), None)
                vcol = next((c for c in ["毛鸡净重(kg)","毛鸡净重","净重","净重(kg)"] if c in df_lw.columns), None)
                if dcol and vcol:
                    _lw = df_lw[[dcol, vcol]].copy()
                    _lw.columns = ["日期","_lw"]
                    _lw["日期"] = pd.to_datetime(_lw["日期"], errors="coerce").dt.normalize()
                    lw_sum = _lw.loc[(_lw["日期"]>=month_start)&(_lw["日期"]<=sel_dt), "_lw"].sum()
            if lw_sum and lw_sum>0:
                cum_final["产成率%"] = (cum_final["产量(kg)"] / lw_sum) * 100.0
        except Exception:
            pass

        for _c in ['产量(kg)','产成率%','含税金额','含税单价']:
            if _c in cum_final.columns:
                cum_final[_c] = pd.to_numeric(cum_final[_c], errors="coerce").round(2)

        st.dataframe(cum_final, use_container_width=True)

if minors is not None and not minors.empty:
    st.subheader("子类")
    days2 = sorted(pd.to_datetime(minors["日期"].dropna().unique()))
    sel2  = st.selectbox("选择日期（下钻）", days2, index=len(days2)-1 if days2 else 0, format_func=lambda d: pd.to_datetime(d).strftime("%Y-%m-%d"))
    for mj in ORDER:
        if mj in ("总计",): continue
        sub = minors[(minors["日期"]==sel2) & (minors["部位大类"]==mj)]
        if sub.empty: continue
        with st.expander(f"{mj} 子类明细", expanded=False):
            sub_disp = sub.copy()
            for _c in ['产量(kg)','产成率%','含税金额','含税单价']:
                if _c in sub_disp.columns:
                    sub_disp[_c] = pd.to_numeric(sub_disp[_c], errors="coerce").round(2)
            if "日期" in sub_disp.columns:
                sub_disp["日期"] = pd.to_datetime(sub_disp["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(sub_disp, use_container_width=True)

# === 新增：当月“无售价”的物料号清单（仅提示，不改计算） ===
st.divider()
st.markdown("#### 当月无售价的物料号")
_show_missing = st.checkbox("显示清单", value=True)

if _show_missing:
    qty_all = read_qty_per_code_per_day(xls)
    pr_all  = build_daily_code_price_raw(xls)

    fallback_days = locals().get('days', locals().get('days2', []))
    _raw = (locals().get('sel') if ('sel' in locals() and str(locals().get('sel')).strip()!="")
            else (fallback_days[-1] if fallback_days else None))

    ts = pd.to_datetime(_raw, errors="coerce")
    if pd.isna(ts):
        st.error("选择的日期无法解析，请检查数据源中的日期格式（建议形如 2025-10-31）。")
        st.stop()

    _ref = None
    if 'sel' in locals():
        _ref = pd.to_datetime(sel, errors="coerce")
    elif 'sel2' in locals():
        _ref = pd.to_datetime(sel2, errors="coerce")
    if (_ref is None) or pd.isna(_ref):
        _ref = pd.to_datetime(overview["日期"], errors="coerce").max()
    if pd.isna(_ref):
        st.warning("没有可用日期，无法生成“当月无售价”的清单。")
        st.stop()

    sel_month = _ref.to_period("M")

    qd  = qty_all.loc[pd.to_datetime(qty_all["日期"]).dt.to_period("M") == sel_month].copy()
    pd0 = pr_all .loc[pd.to_datetime(pr_all ["日期"]).dt.to_period("M") == sel_month].copy()

    if "综合单价" not in pd0.columns:
        pd0["综合单价"] = np.nan
    if "数量" not in pd0.columns:
        pd0["数量"] = 0

    pd0["综合单价"] = pd.to_numeric(pd0["综合单价"], errors="coerce")
    pd0["数量"]    = pd.to_numeric(pd0["数量"],    errors="coerce").fillna(0)

    has_price_mask  = (pd0["数量"] > 0) & pd0["综合单价"].notna() & (pd0["综合单价"] > 0)
    price_codes_month = set(pd0.loc[has_price_mask, "物料号"].dropna().astype(str).unique())

    qd["物料号"] = qd["物料号"].astype(str)
    _missing = (
        qd[["物料号"]].drop_duplicates()
          .loc[lambda d: ~d["物料号"].isin(price_codes_month)]
    )

    if _missing.empty:
        st.success("✅ 当月所有参与产量的物料号均至少有一次有效售价记录。")
    else:
        miss = (
            _missing
            .merge(qd.groupby("物料号", as_index=False)["产量(kg)"].sum(), on="物料号", how="left")
        )
        try:
            miss["部位"] = miss["物料号"].map(code2major).fillna("未映射")
        except Exception:
            miss["部位"] = "未映射"

        miss = (
            miss.assign(_ord=lambda d: (d["部位"] == "未映射").astype(int))
                .sort_values(["_ord", "产量(kg)"], ascending=[False, False])
                .drop(columns="_ord")
        )
        miss["产量(kg)"] = pd.to_numeric(miss["产量(kg)"], errors="coerce").round(2)

        st.dataframe(miss[["物料号", "部位", "产量(kg)"]], use_container_width=True)

# === 新增窗口：自定义部位还原（同日） + 分布后总览 ===
try:
    st.subheader("部位还原")

    if overview is None or (hasattr(overview, "empty") and overview.empty):
        st.info("无数据，无法计算。")
    else:
        _alias_extra = {
            "腿":"腿类","胸":"胸类","里肌":"里肌类","翅":"翅类",
            "鸡骨架":"骨架类","骨架":"骨架类",
            "肉架":"肉架类","鸡架":"肉架类",  # ← 新增：别名统一
            "鸡头":"鸡头类","鸡肝":"鸡肝类","鸡心":"鸡心类",
            "脖":"脖类","鸡脖":"脖类",
            "整鸡":"整鸡类","其他内脏":"其他内脏","下料":"下料类","下料类":"下料类"
        }
        def _unify(x):
            x = str(x)
            try:
                if 'ALIAS' in globals() and x in ALIAS:
                    return ALIAS[x]
            except Exception:
                pass
            return _alias_extra.get(x, x)

        _all_days = sorted(pd.to_datetime(overview["日期"].dropna().unique()))
        if not _all_days:
            st.info("无可用日期。")
            st.stop()
        _latest = max(_all_days)
        _latest_m = pd.Timestamp(_latest).to_period("M")
        _month_days = [d for d in _all_days if pd.Timestamp(d).to_period("M") == _latest_m]
        _first = [d for d in _month_days if pd.Timestamp(d).day == 1]
        _default_ref = _first[0] if _first else _month_days[0]

        ref_day = st.selectbox(
            "还原日（同日口径）",
            _all_days,
            index=_all_days.index(_default_ref),
            format_func=lambda d: pd.to_datetime(d).strftime("%Y-%m-%d")
        )
        ref_day = pd.to_datetime(ref_day).normalize()

        proj_all = [p for p in (ORDER if 'ORDER' in globals() else sorted(overview["项目"].dropna().unique().tolist())) if p != "总计"]

        default_restore = next((p for p in proj_all if "整鸡" in str(p)), (proj_all[0] if proj_all else None))
        restore_part = st.selectbox("需要还原的部位（取当日产量为基数）", proj_all, index=(proj_all.index(default_restore) if default_restore in proj_all else 0))

        common_targets = ["腿类","胸类","里肌类","翅类","骨架类","肉架类",  # ← 新增“肉架类”
                          "爪类","鸡肝类","鸡心类","鸡头类","脖类","鸡胗类","油类","下料类","其他内脏","整鸡类"]
        default_targets = [p for p in proj_all if p in common_targets]
        rate_parts = st.multiselect("选择用于相乘的部位产成率（可多选）", proj_all, default=default_targets)

        if not rate_parts:
            st.warning("请选择至少一个用于相乘的部位产成率。")
            st.stop()

        lw_val = float("nan")
        try:
            if df_lw is not None and not df_lw.empty:
                dcol = next((c for c in ["日期","交鸡日期","记帐日期","记账日期","凭证日期","过账日期"] if c in df_lw.columns), None)
                vcol = next((c for c in ["毛鸡净重(kg)","毛鸡净重","净重","净重(kg)"] if c in df_lw.columns), None)
                if dcol and vcol:
                    _lw = df_lw[[dcol, vcol]].copy()
                    _lw.columns = ["日期","_lw"]
                    _lw["日期"] = pd.to_datetime(_lw["日期"], errors="coerce").dt.normalize()
                    lw_val = _lw.loc[_lw["日期"] == ref_day, "_lw"].sum()
        except Exception:
            pass
        if not (pd.notna(lw_val) and float(lw_val) > 0):
            st.warning("⚠️ 当日没有可用的毛鸡净重，无法计算产成率。")
            st.stop()

        ov_day = overview[overview["日期"] == ref_day][["项目","产量(kg)"]].copy()
        try:
            _ov_amt = overview[overview["日期"] == ref_day][["项目","含税金额"]].copy()
            _ov_amt["项目"] = _ov_amt["项目"].map(_unify)
            amt_map = dict(_ov_amt.groupby("项目", as_index=False)["含税金额"].sum().values.tolist())
        except Exception:
            amt_map = {}
        try:
            _ov_price = overview[overview["日期"] == ref_day][["项目","含税单价"]].copy()
            _ov_price["项目"] = _ov_price["项目"].map(_unify)
            price_map = dict(zip(_ov_price["项目"], _ov_price["含税单价"]))
        except Exception:
            price_map = {}

        if ov_day.empty:
            st.info("该日没有总览产量数据。")
            st.stop()
        ov_day["项目"] = ov_day["项目"].map(_unify)

        src_qty = ov_day.loc[ov_day["项目"] == restore_part, "产量(kg)"].sum()
        if not (pd.notna(src_qty) and float(src_qty) > 0):
            st.warning(f"⚠️ 还原部位在该日没有可用的产量。")
            st.stop()

        rate_df = (ov_day[ov_day["项目"].isin(rate_parts)]
                   .groupby("项目", as_index=False)["产量(kg)"].sum())
        if rate_df.empty:
            st.warning("所选部位在该日均无产量，无法得到产成率。")
            st.stop()
        rate_df["产成率(小数)"] = rate_df["产量(kg)"] / lw_val

        EXCLUDE_PARTS = set()
        rate_df_used = rate_df.copy()

        sum_rate = float(rate_df_used["产成率(小数)"].sum())
        total_restore = float(src_qty * sum_rate)
        normalized_restore = (total_restore / sum_rate) if sum_rate > 0 else 0.0

        det_disp = rate_df_used[["项目","产量(kg)","产成率(小数)"]].copy()
        det_disp = det_disp.rename(columns={"项目":"部位（用于乘产成率）","产量(kg)":"当日部位产量(kg)"})
        det_disp["产成率(%)"] = (det_disp["产成率(小数)"] * 100).round(4)
        det_disp = det_disp[["部位（用于乘产成率）","当日部位产量(kg)","产成率(%)"]]

        st.markdown(f"**当日毛鸡净重：{lw_val:,.2f} kg**")
        st.markdown(f"**还原基数（{restore_part} 当日产量）：{src_qty:,.2f} kg**")
        st.dataframe(det_disp, use_container_width=True)

        st.metric("还原产量（求和 ÷ 产成率合计）", f"{normalized_restore:,.2f} kg")
        st.caption(f"Σ产成率：{sum_rate*100:.4f}% ；分配规则：每个部位增量 = 还原产量（求和 ÷ Σ产成率） × (该部位产成率 ÷ Σ产成率)")

        st.download_button(
            "下载自定义还原明细 CSV",
            data=det_disp.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"自定义部位还原明细_{pd.to_datetime(ref_day).strftime('%Y%m%d')}_{restore_part}.csv",
            mime="text/csv",
            key="dl_restore_detail"
        )

        if sum_rate > 0:
            rate_df_used["分配权重"] = rate_df_used["产成率(小数)"] / sum_rate
            rate_df_used["分配增量"] = normalized_restore * rate_df_used["分配权重"]
            inc_map = dict(zip(rate_df_used["项目"], rate_df_used["分配增量"]))
        else:
            inc_map = {}

        ov_raw = ov_day.groupby("项目", as_index=False)["产量(kg)"].sum()
        show_parts = sorted(set(list(ov_raw["项目"].unique()) + list(rate_df_used["项目"].unique()) + [restore_part]))

        rows = []
        for p in show_parts:
            orig_qty = float(ov_raw.loc[ov_raw["项目"] == p, "产量(kg)"].sum()) if p in ov_raw["项目"].values else 0.0
            inc_qty = float(inc_map.get(p, 0.0))

            if p == "整鸡类":
                new_qty = 0.0
                inc_show = -orig_qty
                rate_after = 0.0
            else:
                new_qty = orig_qty + inc_qty
                inc_show = inc_qty
                rate_after = (new_qty / lw_val * 100.0) if lw_val and lw_val > 0 else 0.0

            rows.append({
                "项目": p,
                "原产量(kg)": round(orig_qty, 2),
                "增量(kg)": round(inc_show,2),
                "分布后产量(kg)": round(new_qty, 2),
                "分布后产成率(%)": round(rate_after, 4),
                "分布后产值(含税)": round(float(amt_map.get(p, 0.0)), 2) if "amt_map" in globals() else None,
                "单价": price_map.get(p, None) if "price_map" in globals() else None
            })
        ov_after = pd.DataFrame(rows)

        drop_show = {"鸡头+鸡脖+骨架"}
        ov_show = ov_after[~ov_after["项目"].isin(drop_show)].copy()

        try:
            _order_seq = ORDER if 'ORDER' in globals() else list(ov_show["项目"].unique())
            _order_map = {name: i for i, name in enumerate([x for x in _order_seq if x != "总计"])}
            ov_show["__ord"] = ov_show["项目"].map(_order_map).fillna(9999)
            ov_show = ov_show.sort_values(["__ord", "项目"]).drop(columns="__ord")
        except Exception:
            pass

        # —— 去掉胸类行（仅限“部位还原 → 分布后总览”）——
        ov_show = ov_show[ov_show["项目"] != "胸类"].copy()

        excluded = {'整鸡类','鸡头+鸡脖+骨架','胸类'}
        ov_included = ov_show[~ov_show['项目'].isin(excluded)].copy()

        sum_orig_not_whole = float(ov_included["原产量(kg)"].sum())
        sum_orig_whole = float(ov_show.loc[ov_show["项目"] == "整鸡类", "原产量(kg)"].sum())
        sum_orig_qty = sum_orig_not_whole + sum_orig_whole

        sum_inc = float(ov_show["增量(kg)"].sum())

        sum_dist_qty = float(ov_included["分布后产量(kg)"].sum())
        overall_rate = (sum_dist_qty / lw_val * 100.0) if pd.notna(lw_val) and lw_val > 0 else None

        whole_value = float(ov_show.loc[ov_show["项目"] == "整鸡类", "分布后产值(含税)"].sum()) if "分布后产值(含税)" in ov_show.columns else 0.0
        sum_value = float(ov_included["分布后产值(含税)"].sum()) + whole_value

        _avg_unit = (sum_value / sum_orig_qty) if (pd.notna(sum_orig_qty) and sum_orig_qty > 0) else np.nan

        total_row = pd.DataFrame([{
            "项目": "总计",
            "原产量(kg)": round(sum_orig_qty, 2),
            "增量(kg)": sum_inc,
            "分布后产量(kg)": round(sum_dist_qty, 2),
            "分布后产成率(%)": round(overall_rate, 4) if overall_rate is not None else None,
            "分布后产值(含税)": round(sum_value, 2),
            "单价": _avg_unit
        }])

        st.markdown("**分布后总览表（产量 & 产成率）**")
        final_table = pd.concat([ov_show, total_row], ignore_index=True)

        display_table = final_table.rename(columns={
            "原产量(kg)": "初始产量",
            "增量(kg)": "整鸡产量",
            "分布后产量(kg)": "实际产量",
            "分布后产成率(%)": "产成率",
            "分布后产值(含税)": "产值"
        })
        if "产成率" in display_table.columns:
            display_table["产成率"] = display_table["产成率"].apply(
                lambda v: ("" if pd.isna(v) else f"{float(v):.2f}%")
            )

        def _fmt_price(v):
            try:
                if v is None:
                    return ""
                if isinstance(v, str):
                    s = v.strip()
                    if s == "" or s.lower() == "nan":
                        return ""
                    v = float(s)
                return f"{float(v):.2f}"
            except Exception:
                return ""
        if "单价" in display_table.columns:
            display_table["单价"] = display_table["单价"].apply(_fmt_price)

        st.dataframe(display_table, use_container_width=True)

        if overall_rate is not None:
            st.metric("还原后产成率", f"{overall_rate:.4f}%")

        st.download_button(
            "下载分布后总览 CSV",
            data=display_table.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"分布后总览_{pd.to_datetime(ref_day).strftime('%Y%m%d')}_{restore_part}.csv",
            mime="text/csv",
            key="dl_overview_dist"
        )

except Exception as _e:
    st.error(f"自定义部位还原（同日）模块异常：{_e}")
