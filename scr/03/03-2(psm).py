import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors

engine = create_engine("mysql+mysqlconnector://root:Zyy0726%40zyy@127.0.0.1:3306/uber_ncr")
df = pd.read_sql("SELECT * FROM v_stage3_psm_base", engine)

# 可选：winsorize，减少长尾干扰
q1, q99 = df["avg_vtat"].quantile([0.01, 0.99])
df["avg_vtat_w"] = df["avg_vtat"].clip(q1, q99)

def smd(x_t, x_c):
    vt = np.var(x_t, ddof=1) if len(x_t) > 1 else 0
    vc = np.var(x_c, ddof=1) if len(x_c) > 1 else 0
    denom = np.sqrt((vt + vc) / 2) if (vt + vc) > 0 else 1.0
    return (np.mean(x_t) - np.mean(x_c)) / denom

def run_psm(d):
    d = d.dropna(subset=["is_cancel","treat_vtat_6_10","avg_vtat_w","hour_of_day","dow","vehicle_type","pickup_location"]).copy()
    if d["treat_vtat_6_10"].nunique() < 2:
        return None

    y = d["is_cancel"].astype(int).values
    t = d["treat_vtat_6_10"].astype(int).values

    num_cols = ["hour_of_day", "dow", "ride_distance"]
    cat_cols = ["vehicle_type", "pickup_location"]

    d["ride_distance"] = pd.to_numeric(d["ride_distance"], errors="coerce")
    d[num_cols] = d[num_cols].fillna(d[num_cols].median(numeric_only=True))

    X = d[num_cols + cat_cols]
    pre = ColumnTransformer([
        ("num", "passthrough", num_cols),
        ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), cat_cols)
    ])

    Xp = pre.fit_transform(X)
    if hasattr(Xp, "toarray"):
        Xp = Xp.toarray()

    # 1) 倾向得分
    ps_model = LogisticRegression(max_iter=3000)
    ps_model.fit(Xp, t)
    ps = ps_model.predict_proba(Xp)[:, 1]
    d["ps"] = ps

    # 2) Common support
    ps_t = d.loc[t == 1, "ps"]
    ps_c = d.loc[t == 0, "ps"]
    lo, hi = max(ps_t.min(), ps_c.min()), min(ps_t.max(), ps_c.max())
    d = d[(d["ps"] >= lo) & (d["ps"] <= hi)].copy()

    y = d["is_cancel"].astype(int).values
    t = d["treat_vtat_6_10"].astype(int).values
    ps = d["ps"].values

    idx_t = np.where(t == 1)[0]
    idx_c = np.where(t == 0)[0]
    if len(idx_t) == 0 or len(idx_c) == 0:
        return None

    # 3) 1:1 最近邻匹配（with replacement）
    nn = NearestNeighbors(n_neighbors=1)
    nn.fit(ps[idx_c].reshape(-1, 1))
    _, ind = nn.kneighbors(ps[idx_t].reshape(-1, 1))
    matched_c_idx = idx_c[ind[:, 0]]

    att = (y[idx_t] - y[matched_c_idx]).mean()
    raw_diff = y[idx_t].mean() - y[idx_c].mean()

    # 4) 简单平衡性检查（数值变量 SMD）
    smd_before = {
        "hour_of_day": smd(d.iloc[idx_t]["hour_of_day"], d.iloc[idx_c]["hour_of_day"]),
        "dow": smd(d.iloc[idx_t]["dow"], d.iloc[idx_c]["dow"]),
        "ride_distance": smd(d.iloc[idx_t]["ride_distance"], d.iloc[idx_c]["ride_distance"]),
    }
    smd_after = {
        "hour_of_day": smd(d.iloc[idx_t]["hour_of_day"], d.iloc[matched_c_idx]["hour_of_day"]),
        "dow": smd(d.iloc[idx_t]["dow"], d.iloc[matched_c_idx]["dow"]),
        "ride_distance": smd(d.iloc[idx_t]["ride_distance"], d.iloc[matched_c_idx]["ride_distance"]),
    }

    return {
        "n": len(d),
        "treated_n": int((t == 1).sum()),
        "control_n": int((t == 0).sum()),
        "raw_diff": float(raw_diff),
        "att_psm": float(att),
        "smd_before": smd_before,
        "smd_after": smd_after
    }

# 总体效应
res_all = run_psm(df)
print("=== Overall PSM ===")
print(res_all)

# HTE：高频 vs 低频
for g, name in [(1, "high_freq"), (0, "low_freq")]:
    r = run_psm(df[df["high_freq"] == g].copy())
    print(f"=== Segment PSM: {name} ===")
    print(r)
