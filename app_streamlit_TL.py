# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import unicodedata as ud

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
_TRANSFER_SHEETS = ["销量-转调理品原料","销量-调拨宫产量","销量-调拨","销量-生肉转调理品原料"]
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
        # 可迭代（list/tuple/set/np.array/pd.Series）
        iterator = list(codes)
    except TypeError:
        # 单个值
        iterator = [codes]

    out = []
    for c in iterator:
        if c is None:
            continue
        s = str(c).strip()
        if not s:
            continue
        # 去掉 Excel 导致的形如 '123456.0' 的小数点尾巴
        if '.' in s:
            s = s.split('.')[0]
        out.append(s)
    return out


def _attach_rate_display(df, df_lw):
    """
    只负责在展示前补回“产成率%”列：
    产成率(%) = 该部位(项目)的 产量(kg) / 当日 毛鸡净重(kg) * 100
    仅改显示层，不改内部计算。
    """
    try:
        import pandas as pd
        import numpy as np
        if df is None or len(df)==0: 
            return df
        out = df.copy()
        if "日期" not in out.columns or "产量(kg)" not in out.columns:
            return out
        # 标准化日期
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce").dt.normalize()
        lw = df_lw.copy() if df_lw is not None else None
        if isinstance(lw, pd.DataFrame) and not lw.empty:
            # 容错列名
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
    def _norm(x):
        s = str(x).strip()
        s = ud.normalize('NFKC', s)
        if s.endswith('.0'):
            try: s = str(int(float(s)))
            except: s = s[:-2]
        return s.upper()
    return series.apply(_norm)

# ===== 读取全部 Sheet =====
@st.cache_resource(show_spinner=False)
def read_all_sheets(uploaded):
    if uploaded is None: return None
    raw = uploaded.getvalue()
    try: return pd.ExcelFile(io.BytesIO(raw))
    except Exception: return None
# ===== 读取产量（按日按物料号） =====
def read_qty_per_code_per_day(xls):
    empty = pd.DataFrame(columns=["日期","物料号","产量(kg)"])
    if xls is None:
        return empty

    target_sheet = None
    if "产量" in xls.sheet_names:
        target_sheet = "产量"
    else:
        for name in xls.sheet_names:
            name_str = str(name)
            if any(key in name_str for key in ("产量", "生产", "产出")):
                target_sheet = name
                break

    if target_sheet is None:
        try:
            sheets_preview = ", ".join(map(str, xls.sheet_names[:5]))
        except Exception:
            sheets_preview = ""
        st.warning(f"主数据未找到名为“产量”的工作表（当前工作表：{sheets_preview}...），请确认上传的文件。")
        return empty

    df = pd.read_excel(xls, target_sheet)
    df.columns = [str(c).strip() for c in df.columns]
    dcol = _pick_col(df.columns, ["日期","记帐日期","记账日期","凭证日期","过账日期","单据日期"])
    ccol = _pick_col(df.columns, _CODE_CANDS)
    qcol = _pick_col(df.columns, ["数量(kg)","数量","净重","净重KG","净重 KG","重量","重量(kg)","KG","kg"])
    if not (dcol and ccol and qcol):
        st.warning("“产量”工作表缺少“日期/物料/数量(kg)”列，无法生成总览。")
        return empty
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
    # 胸类-胸皮 强制映射
    _force_put(["ABB0700210118","ABB0700380618","ABB0700493610","ABB0700493638","ABB0900493611",
        "ADB0700000008","ADB0700000018","ADB0700000048","ADB0700000058","ADB0700380610","ADB0900945610"
    ], "胸类-胸皮")
    _force_put(["ABO0100493640","ABO0100493658","ABO0300210118","ABO0300211658","ABO0300493658",
                "ABO0800493648","ABZ0400723608","ADO0100210100","ADO0100380600","ADO0300210100",
                "ADO0300380600","ADO1500945610"], "其他内脏")
    return code2major

# ===== 构建“原始”当日代码单价（销量 & 转调理/调拨宫） =====

def build_daily_code_price_raw(xls):
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
                "转调金额_含税": pd.to_numeric(df[ext], errors="coerce") * 1.09,
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

    comb = comb[["日期","物料号","金额","数量"]]
    comb = comb.groupby(["日期","物料号"], as_index=False)[["金额","数量"]].sum(min_count=1)

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

    # === 本月1号~前一日的“加权平均价”回填（变更点） ===
    code_filled = []
    # 按日聚合金额/数量，确保每日一条
    agg = (pr_day.groupby(["物料号","月","日期"], as_index=False)
                 .agg(金额=("金额","sum"), 数量=("数量","sum")))
    # 仅保留数量>0的有效日记录
    agg = agg[_np.isfinite(agg["数量"]) & (agg["数量"] != 0)]

    # 对每个 (code, month) 计算到“前一日”的累计加权均价
    for (code, month), gneed in out0[out0["综合单价"].isna()].groupby(["物料号","月"]):
        g = agg[(agg["物料号"]==code) & (agg["月"]==month)].sort_values("日期").copy()
        if g.empty:
            # 无任何历史价：先记空，后续大类兜底
            tmp = gneed.copy()
            tmp["综合单价_filled"] = _np.nan
            code_filled.append(tmp[["日期","物料号","综合单价_filled"]])
            continue

        g["cum_amt"] = g["金额"].cumsum()
        g["cum_qty"] = g["数量"].cumsum()
        # 到当天为止的加权均价；后续 merge_asof 用 allow_exact_matches=False 取严格“小于当天”（即前一日）
        g["mtd_wavg"] = _np.where(g["cum_qty"] != 0, g["cum_amt"]/g["cum_qty"], _np.nan)

        gneed_sorted  = gneed.sort_values("日期")
        gprice_sorted = g[["日期","mtd_wavg"]].sort_values("日期")

        filled = _pd.merge_asof(
            gneed_sorted,
            gprice_sorted,
            left_on="日期",
            right_on="日期",
            direction="backward",
            allow_exact_matches=False  # 确保取“前一日”
        )
        filled.rename(columns={"mtd_wavg":"综合单价_filled"}, inplace=True)
        code_filled.append(filled[["日期","物料号","综合单价_filled"]])

    code_filled = _pd.concat(code_filled, ignore_index=True) if code_filled else _pd.DataFrame(columns=["日期","物料号","综合单价_filled"])

    # 优先用当日价，其次用加权均价回填
    out = out0.merge(code_filled, on=["日期","物料号"], how="left")
    out["综合单价_filled"] = out["综合单价"].where(out["综合单价"].notna(), out["综合单价_filled"])

    
    # === 手工补价（位于“当月加权均价”之后、“大类兜底”之前） ===
    if manual_month is not None and not getattr(manual_month, 'empty', True):
        mm = manual_month.copy()
        mm['物料号'] = mm['物料号'].astype(str).str.strip()
        out = out.merge(mm.rename(columns={'手工单价':'_手工单价'}), on='物料号', how='left')
        out['综合单价_filled'] = out['综合单价_filled'].where(out['综合单价_filled'].notna(), out['_手工单价'])
        if '_手工单价' in out.columns:
            out.drop(columns=['_手工单价'], inplace=True)

    # === 大类兜底（保持原口径不变） ===
    pr_raw2 = pr_raw.copy()
    pr_raw2["日期"] = _pd.to_datetime(pr_raw2["日期"], errors="coerce").dt.normalize()
    pr_raw2["部位"] = pr_raw2["物料号"].map(code2major)
    pr_raw2 = pr_raw2[pr_raw2["部位"].notna()]

    # 大类当日加权价（金额/数量）
    cat_day = pr_raw2.groupby(["日期","部位"], as_index=False)[["金额","数量"]].sum()
    cat_day["大类价"] = _np.where(cat_day["数量"] != 0, cat_day["金额"]/cat_day["数量"], _np.nan)

    # 大类近7天滚动加权均价（按当日加权价时间窗口平均）
    cat_day = cat_day.sort_values(["部位","日期"])
    cat_day["大类价_7d"] = _np.nan
    for b, g in cat_day.groupby("部位"):
        s = g.set_index("日期")["大类价"].rolling("7D", min_periods=1).mean()
        cat_day.loc[g.index, "大类价_7d"] = s.values

    # 大类期内中位数
    cat_med = cat_day.groupby("部位", as_index=False)["大类价"].median().rename(columns={"大类价":"大类价_中位"})

    out["部位"] = out["物料号"].map(code2major)
    out = out.merge(cat_day[["日期","部位","大类价","大类价_7d"]], on=["日期","部位"], how="left")
    out = out.merge(cat_med, on="部位", how="left")

    # 兜底顺序：大类当日 → 大类7天 → 大类中位
    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价"])
    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价_7d"])
    out["综合单价_filled"] = out["综合单价_filled"].where(out["综合单价_filled"].notna(), out["大类价_中位"])

    return out[["日期","物料号","综合单价_filled"]]


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
    over["含税单价"] = np.where(over["产量(kg)"] != 0, over["含税金额"]/over["产量(kg)"], np.nan)
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
    minors["含税单价"] = np.where(minors["产量(kg)"] != 0, minors["含税金额"]/minors["产量(kg)"], np.nan)
    return over, minors

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
# ===== 读取“部位分摊表”并标准化为长表 =====

@st.cache_data(show_spinner=False)
def read_part_allocation(uploaded):
    """
    输入：分摊表（Excel/CSV）。推荐列：日期(可选)、物料号(必填)、部位列(若干)、合计(可选)；部位列可为百分比或0~1。
    输出：长表：['日期','物料号','项目','权重']；其中“项目”为模型的大类名。
    匹配优先级：按日(日期+物料号) > 通用(仅物料号)。
    规则：填写“日期”→仅该日生效；未填写→默认通用（当月所有日期生效）。
    """
    import pandas as pd, io as _io

    if uploaded is None:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    raw = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
    try:
        if (getattr(uploaded, "type", "") or "").endswith("csv") or str(getattr(uploaded, "name", "")).lower().endswith(".csv"):
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

    # 识别分摊列（排除键列/描述列）
    exclude = {dcol, ccol, "公司","工厂","物料描述","品类","序号","部位","合计", None}
    part_cols = [c for c in df.columns if c not in exclude]

    # 列名映射到模型大类（含“鸡骨架/腿骨泥/半架”统一到 骨架类；“下料/地脚料”统一到 下料类）
    part2major = {
        "全腿":"腿类", "腿":"腿类",
        "胸肉块":"胸类-胸","胸":"胸类-胸","胸类-胸":"胸类-胸","胸类-胸皮":"胸类-胸皮",
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
        "其他内脏":"其他内脏"
    }

    keep_pairs = []
    for c in part_cols:
        std = part2major.get(c, c)  # 未在词典中的，按原名尝试（若已是模型标准名会保留）
        keep_pairs.append((c, std))
    if not keep_pairs:
        return pd.DataFrame(columns=["日期","物料号","项目","权重"])

    # —— 关键：安全解析日期（含 Excel 数值日期），避免对 NaT 调 .normalize() ——
    def _safe_day(x):
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip()==""):
            return pd.NaT
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            # Excel 序列日期：1899-12-30 起算
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
                if ("%" in s) or v>1.001:  # 兼容百分比/100制
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
        # 行内归一化（容差自动放大到1）
        weights = [(name, v/tot) for name, v in weights]

        for name, v in weights:
            rows.append({"日期": day, "物料号": code, "项目": name, "权重": float(v)})

    out = pd.DataFrame(rows, columns=["日期","物料号","项目","权重"])
    if not out.empty:
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce")  # 不再 .dt.normalize()，已在 _safe_day 处理
    return out


@st.cache_data(show_spinner=False)
def read_restore_config(_xls):
    import pandas as _pd
    if _xls is None:
        return _pd.DataFrame(columns=["需要还原的部位","品项","原部位","权重值","原部位_标准"])
    try:
        if "部位还原配置" not in _xls.sheet_names:
            return _pd.DataFrame(columns=["需要还原的部位","品项","原部位","权重值","原部位_标准"])
        df = _pd.read_excel(_xls, "部位还原配置")
        df.columns = [str(c).strip() for c in df.columns]
        # 允许列名的宽松匹配
        col_map = {}
        for k in ["需要还原的部位","品项","原部位","权重值","原部位_标准"]:
            for c in df.columns:
                if str(c).strip() == k:
                    col_map[k] = c
                    break
        # 必须列
        need = ["需要还原的部位","品项","原部位","权重值"]
        if not all(k in col_map for k in need):
            return _pd.DataFrame(columns=["需要还原的部位","品项","原部位","权重值","原部位_标准"])
        out = _pd.DataFrame({
            "需要还原的部位": df[col_map["需要还原的部位"]].astype(str).str.strip(),
            "品项": df[col_map["品项"]].astype(str).str.strip(),
            "原部位": df[col_map["原部位"]].astype(str).str.strip(),
            "权重值": _pd.to_numeric(df[col_map["权重值"]], errors="coerce"),
        })
        if "原部位_标准" in col_map:
            out["原部位_标准"] = df[col_map["原部位_标准"]].astype(str).str.strip().where(df[col_map["原部位_标准"]].notna(), None)
        else:
            out["原部位_标准"] = None
        out = out.dropna(subset=["需要还原的部位","品项","原部位","权重值"])
        out = out[out["权重值"]!=0]
        # 别名：将“其中：胸部/胸皮”规范为胸类-胸/胸类-胸皮（若未提供标准列）
        alias = {"其中：胸部":"胸类-胸","其中：胸皮":"胸类-胸皮"}
        out["原部位_标准"] = out["原部位_标准"].where(out["原部位_标准"].notna() & (out["原部位_标准"]!=""), out["原部位"].map(alias))
        return out
    except Exception:
        return _pd.DataFrame(columns=["需要还原的部位","品项","原部位","权重值","原部位_标准"])


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
        # 关键：去掉 mon_alloc 的“日期”列，避免 merge 后出现 日期_x/日期_y
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


    base = m_code_daily.copy()
    base["含税金额"] = base["产量(kg)"] * base["综合单价_filled"]

    if alloc_long is None or alloc_long.empty:
        base["项目"] = base["物料号"].map(code2major)
        return base[["日期","项目","物料号","产量(kg)","含税金额"]]

    day_alloc = alloc_long[alloc_long["日期"].notna()].copy()
    mon_alloc = alloc_long[alloc_long["日期"].isna()].copy()

    parts = []

    # 1) 按日规则
    if not day_alloc.empty:
        a = base.merge(day_alloc, on=["日期","物料号"], how="inner")
        if not a.empty:
            a["产量(kg)"] = a["产量(kg)"] * a["权重"]
            a["含税金额"] = a["含税金额"] * a["权重"]
            parts.append(a[["日期","项目","物料号","产量(kg)","含税金额"]])

    # 2) 通用规则（对未覆盖的剩余）
    if not mon_alloc.empty:
        if parts:
            used = pd.concat(parts, ignore_index=True)[["日期","物料号"]].drop_duplicates()
            rem = base.merge(used, on=["日期","物料号"], how="left", indicator=True)
            rem = rem[rem["_merge"]=="left_only"].drop(columns="_merge")
        else:
            rem = base
        b = rem.merge(mon_alloc, on=["物料号"], how="inner")
        if not b.empty:
            b["产量(kg)"] = b["产量(kg)"] * b["权重"]
            b["含税金额"] = b["含税金额"] * b["权重"]
            parts.append(b[["日期","项目","物料号","产量(kg)","含税金额"]])

    # 3) 仍未命中 → 一对一映射
    if parts:
        covered = pd.concat(parts, ignore_index=True)[["日期","物料号"]].drop_duplicates()
        rest = base.merge(covered, on=["日期","物料号"], how="left", indicator=True)
        rest = rest[rest["_merge"]=="left_only"].drop(columns="_merge")
    else:
        rest = base

    if not rest.empty:
        rest["项目"] = rest["物料号"].map(code2major)
        parts.append(rest[["日期","项目","物料号","产量(kg)","含税金额"]])

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["日期","项目","物料号","产量(kg)","含税金额"])

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
    st.error("主数据中未读取到任何有效的产量记录，请检查“产量”工作表及列名（日期/物料/数量）。")
    st.stop()
else:
    days = sorted(pd.to_datetime(overview["日期"].dropna().unique()))
    sel  = st.selectbox("选择日期", days, index=len(days)-1 if days else 0, format_func=lambda d: pd.to_datetime(d).strftime("%Y-%m-%d"))
    ov = overview[overview["日期"]==sel].copy()

    must = pd.DataFrame({"日期": sel, "项目": SPECIFIED})
    ov = must.merge(ov, on=["日期","项目"], how="left")
    for c in ["产量(kg)","产成率%","含税金额","含税单价"]:
        if c not in ov.columns: ov[c]=np.nan
    ov["产量(kg)"]=ov["产量(kg)"].fillna(0.0)
    ov["含税金额"]=ov["含税金额"].fillna(0.0)
    ov["含税单价"]=ov["含税单价"].fillna(0.0)

    others = overview[(overview["日期"]==sel) & (~overview["项目"].isin(SPECIFIED))].copy()
    try:
        others["项目"]=pd.Categorical(others["项目"],categories=[x for x in ORDER if x not in SPECIFIED],ordered=True)
        others=others.sort_values("项目")
    except Exception: pass

    # 总计（只汇总基础大类）
    base_today = pd.concat([ov, others], ignore_index=True)
    dynamic_base = set(BASE_FOR_TOTAL)
    # —— 互斥：若“胸类”在当日集合中，则排除“胸类-胸/胸类-胸皮”；否则排除“胸类” ——
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

    total_row = pd.DataFrame({"日期":[sel],"项目":["总计"],                              "产量(kg)":[float(tot_qty) if pd.notna(tot_qty) else 0.0],                              "含税金额":[float(tot_amt) if pd.notna(tot_amt) else 0.0],                              "含税单价":[float(tot_unit)],"产成率%":[np.nan]})
    final_ov = pd.concat([base_today, total_row], ignore_index=True)
    disp = _attach_rate_display(final_ov, df_lw).copy()
    for _c in ['产量(kg)','产成率%','含税金额','含税单价']:
        if _c in disp.columns:
            disp[_c] = disp[_c].round(2)
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

        # 选取本月从1号到所选日（含）的区间
        rng = (overview["日期"] >= month_start) & (overview["日期"] <= sel_dt)
        cum = overview.loc[rng].groupby("项目", as_index=False).agg({"产量(kg)":"sum","含税金额":"sum"})
        cum["含税单价"] = np.where(cum["产量(kg)"] != 0, cum["含税金额"]/cum["产量(kg)"], np.nan)

        # 必显顺序 + 补零
        must_c = pd.DataFrame({"项目": SPECIFIED})
        cum = must_c.merge(cum, on="项目", how="left")
        for c in ["产量(kg)","含税金额","含税单价"]:
            if c not in cum.columns: 
                cum[c] = np.nan
        cum["产量(kg)"] = pd.to_numeric(cum["产量(kg)"], errors="coerce").fillna(0.0)
        cum["含税金额"] = pd.to_numeric(cum["含税金额"], errors="coerce").fillna(0.0)
        cum["含税单价"] = pd.to_numeric(cum["含税单价"], errors="coerce").fillna(0.0)

        # 其它大类（非必显）
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

        # 总计（仅基础大类）
        dynamic_base_cum = set(BASE_FOR_TOTAL)
        # —— 互斥：若区间含“胸类”，则排除“胸类-胸/胸类-胸皮”；否则排除“胸类” ——
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

        total_row = pd.DataFrame({"项目":["总计"],                               "产量(kg)":[float(tot_qty) if pd.notna(tot_qty) else 0.0],                               "含税金额":[float(tot_amt) if pd.notna(tot_amt) else 0.0],                               "含税单价":[float(tot_unit)]})
        cum_final = pd.concat([cum_base, total_row], ignore_index=True)

        # 可选：累计产成率（以本月毛鸡净重累计为分母）
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
                    sub_disp[_c] = sub_disp[_c].round(2)
            if "日期" in sub_disp.columns:
                sub_disp["日期"] = pd.to_datetime(sub_disp["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(sub_disp, use_container_width=True)
# === 新增：当天“无售价”的物料号清单（仅提示，不改计算；避免嵌套 expander） ===
st.divider()
st.markdown("#### 当月无售价的物料号")
_show_missing = st.checkbox("显示清单", value=True)

if _show_missing:
    if overview is None or overview.empty:
        st.warning("没有总览数据，无法生成“当月无售价”的清单。")
    else:
        # 全量（当月范围）
        qty_all = read_qty_per_code_per_day(xls)
        pr_all  = build_daily_code_price_raw(xls)

        # 选定参考月份：优先 sel / sel2，其次最新日期
        sel_month = resolve_sel_month(overview)
        if sel_month is None:
            st.warning("无法识别可用日期，无法生成“当月无售价”的清单。")
            st.stop()

        qd  = qty_all.loc[pd.to_datetime(qty_all["日期"]).dt.to_period("M") == sel_month].copy()
        pd0 = pr_all .loc[pd.to_datetime(pr_all ["日期"]).dt.to_period("M") == sel_month].copy()

        # 容错：缺列时补空列；统一数值类型
        if "综合单价" not in pd0.columns:
            pd0["综合单价"] = np.nan
        if "数量" not in pd0.columns:
            pd0["数量"] = 0

        pd0["综合单价"] = pd.to_numeric(pd0["综合单价"], errors="coerce")
        pd0["数量"]    = pd.to_numeric(pd0["数量"],    errors="coerce").fillna(0)

        # “有售价”口径（保持原口径不变）：数量>0 且 单价存在且>0
        has_price_mask  = (pd0["数量"] > 0) & pd0["综合单价"].notna() & (pd0["综合单价"] > 0)
        # 在本月任一日有售价即视为“本月有售价”
        price_codes_month = set(pd0.loc[has_price_mask, "物料号"].dropna().astype(str).unique())

        # 在产量里出现、但整月没有任何有效售价的物料号
        qd["物料号"] = qd["物料号"].astype(str)
        _missing = (
            qd[["物料号"]].drop_duplicates()
              .loc[lambda d: ~d["物料号"].isin(price_codes_month)]
        )

        if _missing.empty:
            st.success("✅ 当月所有参与产量的物料号均至少有一次有效售价记录。")
        else:
            # 展示：部位映射 & 本月合计产量
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
            "鸡头":"鸡头类","鸡肝":"鸡肝类","鸡心":"鸡心类",
            "脖":"脖类","鸡脖":"脖类",
            "整鸡":"整鸡类","其他内脏":"其他内脏","下料":"下料类","下料类":"下料类"
        }

        def _unify(name: str) -> str:
            x = str(name)
            try:
                if 'ALIAS' in globals() and x in ALIAS:
                    return ALIAS[x]
            except Exception:
                pass
            return _alias_extra.get(x, x)

        def _parse_mapping(text_value: str):
            mapping = {}
            for line in text_value.splitlines():
                row = line.strip()
                if not row or row.startswith(('#', '/')):
                    continue
                token = None
                if ':' in row:
                    token = ':'
                elif '->' in row:
                    token = '->'
                if token is None:
                    continue
                code, targets = row.split(token, 1)
                code = code.strip()
                targets = targets.strip()
                if not code or not targets:
                    continue
                pieces = [t.strip() for t in re.split(r"[，,;；/|]", targets) if t.strip()]
                if pieces:
                    mapping[code] = pieces
            return mapping

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
        restore_parts = st.multiselect(
            "需要还原的部位",
            proj_all,
            default=[default_restore] if default_restore else (proj_all[:1] if proj_all else [])
        )
        if not restore_parts:
            st.warning("请选择至少一个需要还原的部位。")
            st.stop()

        rate_parts = st.multiselect(
            "作为目标的部位（用于获取产成率）",
            proj_all,
            default=[p for p in proj_all if any(k in p for k in ["腿","胸","里肌","翅","骨架","爪","肝","心","脖","胗","油","下料"]) ]
        )
        if not rate_parts:
            st.warning("至少选择一个目标部位。")
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

        ov_day = overview[overview["日期"] == ref_day][["项目","产量(kg)","含税金额","含税单价"]].copy()
        if ov_day.empty:
            st.info("该日没有总览产量数据。")
            st.stop()
        ov_day["项目"] = ov_day["项目"].map(_unify)

        amt_map = dict(ov_day.groupby("项目", as_index=False)["含税金额"].sum().values.tolist()) if "含税金额" in ov_day.columns else {}
        price_map = dict(zip(ov_day["项目"], ov_day.get("含税单价", np.nan)))

        rate_df = (ov_day[ov_day["项目"].isin(rate_parts)]
                   .groupby("项目", as_index=False)["产量(kg)"].sum())
        if rate_df.empty:
            st.warning("目标部位在该日没有产量，无法计算产成率。")
            st.stop()
        rate_df["产成率(小数)"] = rate_df["产量(kg)"] / lw_val
        rate_dict = {row["项目"]: row["产成率(小数)"] for _, row in rate_df.iterrows() if pd.notna(row["产成率(小数)"]) and row["产成率(小数)"] > 0}
        if not rate_dict:
            st.warning("所有目标部位产成率为 0，无法分配。")
            st.stop()

        global_detail_records = []
        global_code_sequence = []
        global_removed_map = {}

        for restore_part in restore_parts:
            st.markdown(f"### {restore_part}")

            minors_today = pd.DataFrame()
            if minors is not None and not minors.empty:
                try:
                    day_norm = pd.to_datetime(minors["日期"], errors="coerce").dt.normalize()
                    mask = (day_norm == ref_day) & (minors["部位大类"] == restore_part)
                    minors_today = minors.loc[mask].copy()
                except Exception:
                    minors_today = pd.DataFrame()

            if minors_today.empty:
                st.info("该部位在所选日期无子类明细，跳过。")
                continue

            code_col = None
            for cand in ["子类","品项","物料号","名称","物料"]:
                if cand in minors_today.columns:
                    code_col = cand
                    break
            if code_col is None:
                st.warning("子类表缺少物料列，无法继续。")
                continue

            minors_today[code_col] = minors_today[code_col].astype(str).str.strip()
            available_codes = sorted({c for c in minors_today[code_col] if c and c.lower() != "nan"})
            if not available_codes:
                st.warning("该部位没有可用的物料号。")
                continue

            selected_codes = st.multiselect(
                "选择需要还原的物料号",
                available_codes,
                default=available_codes,
                help="可多选；仅处理所选物料的产量。",
                key=f"codes_{restore_part}"
            )
            if not selected_codes:
                st.warning("至少选择一个物料号。")
                continue

            for _code in selected_codes:
                if _code not in global_code_sequence:
                    global_code_sequence.append(_code)

            item_target_map = {}
            with st.expander("配置每个物料号要还原到的部位", expanded=False):
                target_choices = proj_all if proj_all else rate_parts
                for code in selected_codes:
                    if rate_parts:
                        default_targets = rate_parts[:3] if len(rate_parts) >= 3 else rate_parts
                    else:
                        default_targets = target_choices[:3] if target_choices else []
                    selected_targets = st.multiselect(
                        f"{code} → 目标部位",
                        target_choices,
                        default=default_targets,
                        key=f"map_{restore_part}_{code}"
                    )
                    item_target_map[code] = selected_targets

            raw_mapping = {code: item_target_map.get(code, rate_parts) for code in selected_codes}

            base_qty = float(ov_day.loc[ov_day["项目"] == restore_part, "产量(kg)"].sum())
            if not (pd.notna(base_qty) and base_qty > 0):
                st.warning(f"还原部位【{restore_part}】在该日没有可用的产量。")
                continue

            subset = minors_today[minors_today[code_col].isin(selected_codes)].copy()
            subset["产量(kg)"] = pd.to_numeric(subset["产量(kg)"], errors="coerce").fillna(0.0)
            qty_series = subset.groupby(code_col)["产量(kg)"].sum()
            item_qty_map = {k: float(v) for k, v in qty_series.items() if pd.notna(v)}
            if not item_qty_map:
                st.warning("所选物料在该日没有产量。")
                continue

            detail_rows = []
            inc_map = {}
            removed_qty = 0.0
            for code, qty in item_qty_map.items():
                targets = raw_mapping.get(code, raw_mapping.get(code.upper(), []))
                if not targets:
                    targets = rate_parts
                targets = [_unify(t) for t in targets]
                valid_targets = [t for t in targets if rate_dict.get(t, 0) > 0]
                if not valid_targets:
                    continue
                total_rate = sum(rate_dict[t] for t in valid_targets)
                if total_rate <= 0:
                    continue
                removed_qty += qty
                for t in valid_targets:
                    share = rate_dict[t] / total_rate
                    inc = qty * share
                    inc_map[t] = inc_map.get(t, 0.0) + inc
                    detail_rows.append({
                        "物料号": code,
                        "源产量(kg)": qty,
                        "目标部位": t,
                        "目标产成率(%)": rate_dict[t] * 100.0,
                        "分配比例(%)": share * 100.0,
                        "增量(kg)": inc
                    })

            if not detail_rows:
                st.warning("映射结果为空，请检查目标部位设置或当日产成率。")
                continue

            global_detail_records.extend(detail_rows)
            global_removed_map[restore_part] = global_removed_map.get(restore_part, 0.0) + removed_qty

            detail_df = pd.DataFrame(detail_rows)
            detail_df["源产量(kg)"] = detail_df["源产量(kg)"].round(2)
            detail_df["增量(kg)"] = detail_df["增量(kg)"].round(2)
            detail_df["目标产成率(%)"] = detail_df["目标产成率(%)"].round(4)
            detail_df["分配比例(%)"] = detail_df["分配比例(%)"].round(4)

            try:
                pivot_df = detail_df.pivot_table(
                    index="目标部位",
                    columns="物料号",
                    values="增量(kg)",
                    aggfunc="sum",
                    fill_value=0.0
                )
                if not pivot_df.empty:
                    row_order = []
                    if 'ORDER' in globals():
                        row_order = [p for p in ORDER if p in pivot_df.index]
                    remainder = [p for p in pivot_df.index if p not in row_order]
                    row_order.extend(remainder)
                    pivot_df = pivot_df.reindex(row_order)
                    col_order = [c for c in selected_codes if c in pivot_df.columns]
                    col_order += [c for c in pivot_df.columns if c not in col_order]
                    pivot_df = pivot_df[col_order]
                    pivot_df["合计(kg)"] = pivot_df.sum(axis=1)
                    pivot_df.loc["总计"] = pivot_df.sum(axis=0)
                    pivot_df = pivot_df.round(2)
                    st.markdown("**物料号 × 部位还原总览**")
                    st.dataframe(pivot_df, use_container_width=True)
            except Exception:
                pass

            st.markdown(f"**当日毛鸡净重：{lw_val:,.2f} kg**")
            st.markdown(f"**选中物料合计产量：{removed_qty:,.2f} kg**")
            st.dataframe(detail_df, use_container_width=True)
            st.download_button(
                "下载物料拆分明细",
                data=detail_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name=f"物料拆分明细_{pd.to_datetime(ref_day).strftime('%Y%m%d')}_{restore_part}.csv",
                mime="text/csv",
                key=f"dl_restore_detail_{restore_part}"
            )

            ov_raw = ov_day.groupby("项目", as_index=False)["产量(kg)"].sum()
            show_parts = sorted(set(list(ov_raw["项目"].unique()) + list(inc_map.keys()) + [restore_part]))

            rows = []
            for p in show_parts:
                orig_qty = float(ov_raw.loc[ov_raw["项目"] == p, "产量(kg)"].sum()) if p in ov_raw["项目"].values else 0.0
                if p == restore_part:
                    inc_qty = -removed_qty
                else:
                    inc_qty = inc_map.get(p, 0.0)
                new_qty = max(orig_qty + inc_qty, 0.0)
                rate_after = (new_qty / lw_val * 100.0) if lw_val and lw_val > 0 else 0.0
                rows.append({
                    "项目": p,
                    "原产量(kg)": round(orig_qty, 2),
                    "增量(kg)": round(inc_qty, 2),
                    "分布后产量(kg)": round(new_qty, 2),
                    "分布后产成率(%)": round(rate_after, 4),
                    "分布后产值(含税)": round(float(amt_map.get(p, 0.0)), 2) if isinstance(amt_map, dict) else None,
                    "单价": price_map.get(p)
                })

            ov_after = pd.DataFrame(rows)

        if global_detail_records:
            try:
                total_detail_df = pd.DataFrame(global_detail_records)
                pivot_all = total_detail_df.pivot_table(
                    index="目标部位",
                    columns="物料号",
                    values="增量(kg)",
                    aggfunc="sum",
                    fill_value=0.0
                )
                if not pivot_all.empty:
                    row_order = []
                    if 'ORDER' in globals():
                        row_order = [p for p in ORDER if p in pivot_all.index]
                    remainder = [p for p in pivot_all.index if p not in row_order]
                    row_order.extend(remainder)
                    pivot_all = pivot_all.reindex(row_order)
                    if global_code_sequence:
                        col_order = [c for c in global_code_sequence if c in pivot_all.columns]
                    else:
                        col_order = []
                    col_order += [c for c in pivot_all.columns if c not in col_order]
                    pivot_all = pivot_all[col_order]
                    pivot_all["合计(kg)"] = pivot_all.sum(axis=1)
                    pivot_all.loc["总计"] = pivot_all.sum(axis=0)
                    pivot_all = pivot_all.round(2)
                    st.markdown("### 组合部位还原总览（全部物料号）")
                    st.dataframe(pivot_all, use_container_width=True)

                base_summary = ov_day.groupby("项目", as_index=False)["产量(kg)"].sum()
                base_qty_map = {str(row["项目"]): float(row["产量(kg)"]) for _, row in base_summary.iterrows()}
                inc_summary = total_detail_df.groupby("目标部位", as_index=False)["增量(kg)"].sum()
                inc_qty_map = {str(row["目标部位"]): float(row["增量(kg)"]) for _, row in inc_summary.iterrows()}
                show_projects = set(list(base_qty_map.keys()) + list(inc_qty_map.keys()) + list(global_removed_map.keys()))
                if show_projects:
                    row_order = []
                    if 'ORDER' in globals():
                        row_order = [p for p in ORDER if p in show_projects]
                    remainder = [p for p in show_projects if p not in row_order]
                    remainder.sort()
                    row_order.extend(remainder)

                    ov_rows = []
                    for proj in row_order:
                        orig_qty = base_qty_map.get(proj, 0.0)
                        inc_qty = inc_qty_map.get(proj, 0.0) - global_removed_map.get(proj, 0.0)
                        new_qty = max(orig_qty + inc_qty, 0.0)
                        rate_after = (new_qty / lw_val * 100.0) if (pd.notna(lw_val) and lw_val > 0) else np.nan
                        ov_rows.append({
                            "项目": proj,
                            "原产量(kg)": round(orig_qty, 2),
                            "调整量(kg)": round(inc_qty, 2),
                            "调整后产量(kg)": round(new_qty, 2),
                            "调整后产成率(%)": rate_after
                        })

                    summary_df = pd.DataFrame(ov_rows)
                    if not summary_df.empty:
                        exclude_for_sum = {"胸类","鸡头+鸡脖+骨架"}
                        sum_base = summary_df[~summary_df["项目"].isin(exclude_for_sum)]
                        total_row = {
                            "项目": "总计",
                            "原产量(kg)": round(float(sum_base["原产量(kg)"].sum()), 2),
                            "调整量(kg)": round(float(sum_base["调整量(kg)"].sum()), 2),
                            "调整后产量(kg)": round(float(sum_base["调整后产量(kg)"].sum()), 2),
                        }
                        exclude_for_rate = {"胸类","鸡头+鸡脖+骨架"}
                        base_for_rate = summary_df[~summary_df["项目"].isin(exclude_for_rate)]["调整后产量(kg)"]
                        adj_qty_for_rate = float(base_for_rate.sum()) if not base_for_rate.empty else 0.0
                        tot_rate = (adj_qty_for_rate / lw_val * 100.0) if (pd.notna(lw_val) and lw_val > 0) else np.nan
                        total_row["调整后产成率(%)"] = tot_rate
                        summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)
                        display_global = summary_df.copy()
                        display_global["调整后产成率(%)"] = display_global["调整后产成率(%)"].apply(
                            lambda v: "" if pd.isna(v) else f"{float(v):.2f}%"
                        )
                        st.markdown("### 组合还原后产成率总览")
                        st.dataframe(display_global, use_container_width=True)
            except Exception:
                pass

except Exception as _e:
    st.error(f"自定义部位还原（同日）模块异常：{_e}")
