# 04-2(Real_Strategy_Experiment).py
# Dynamic Fulfillment Intervention - key metrics only

import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

RANDOM_SEED = 42
RESCUE_SCENARIOS = [0.05, 0.06, 0.07, 0.08]  # 5%~8%
COUPON_COST = 5.0
DB_URL = "mysql+mysqlconnector://root:Zyy0726%40zyy@127.0.0.1:3306/uber_ncr"

def norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

def diff_in_rate_ci(y_t: pd.Series, y_c: pd.Series):
    p_t, p_c = y_t.mean(), y_c.mean()
    n_t, n_c = len(y_t), len(y_c)
    delta = p_c - p_t  # 正值=实验组取消率更低
    se = math.sqrt(p_t * (1 - p_t) / n_t + p_c * (1 - p_c) / n_c)
    ci_l, ci_u = delta - 1.96 * se, delta + 1.96 * se
    z = (p_t - p_c) / se if se > 0 else 0.0
    p = 2.0 * (1.0 - norm_cdf(abs(z)))
    return float(delta), float(ci_l), float(ci_u), float(p)

def run_one_scenario(df: pd.DataFrame, rescue_rate: float, seed: int = 42):
    rng = np.random.default_rng(seed)

    # 仅目标人群：高频 + VTAT[4,8)
    g = df[df["eligible_dfi"] == 1].copy()
    if g.empty:
        raise ValueError("No eligible_dfi samples found in v_stage4_dfi_base.")

    # A/B 分组（模拟）
    g["treat"] = rng.integers(0, 2, size=len(g))  # 0=Ctrl, 1=Treat
    g["y0_cancel"] = g["is_cancel"].astype(int)
    g["y1_cancel"] = g["y0_cancel"].copy()

    # 干预生效：实验组取消单按 rescue_rate 被挽回
    mask_cancel_treat = (g["treat"] == 1) & (g["y0_cancel"] == 1)
    rescue_draw = rng.random(mask_cancel_treat.sum()) < rescue_rate
    rescued_idx = g.index[mask_cancel_treat][rescue_draw]
    g.loc[rescued_idx, "y1_cancel"] = 0

    # Primary
    y_t = g.loc[g["treat"] == 1, "y1_cancel"]
    y_c = g.loc[g["treat"] == 0, "y1_cancel"]
    delta, ci_l, ci_u, p_value = diff_in_rate_ci(y_t, y_c)

    # Business
    recovered_orders = int(((g["y0_cancel"] == 1) & (g["y1_cancel"] == 0)).sum())
    avg_order_value = float(df.loc[df["booking_status"] == "Completed", "booking_value"].dropna().mean())
    gmv_recovered = recovered_orders * avg_order_value
    coupon_spend = recovered_orders * COUPON_COST
    net_gmv = gmv_recovered - coupon_spend

    # Guardrail（这里用平均VTAT差）
    avg_vtat_ctrl = float(g.loc[g["treat"] == 0, "avg_vtat"].mean())
    avg_vtat_treat = float(g.loc[g["treat"] == 1, "avg_vtat"].mean())
    guardrail_vtat_diff = avg_vtat_treat - avg_vtat_ctrl

    return {
        "rescue_rate": rescue_rate,
        "delta_cancel_reduction": delta,
        "ci95_l": ci_l,
        "ci95_u": ci_u,
        "p_value": p_value,
        "net_gmv": net_gmv,
        "guardrail_vtat_diff": guardrail_vtat_diff,
    }

def main():
    engine = create_engine(DB_URL)
    df = pd.read_sql("SELECT * FROM v_stage4_dfi_base", engine)

    results = [run_one_scenario(df, rr, RANDOM_SEED) for rr in RESCUE_SCENARIOS]
    out = pd.DataFrame(results)

    # 只输出关键指标
    out = out[
        [
            "rescue_rate",
            "delta_cancel_reduction",
            "ci95_l",
            "ci95_u",
            "p_value",
            "net_gmv",
            "guardrail_vtat_diff",
        ]
    ]

    pd.set_option("display.width", 200)
    print(out.to_string(index=False, float_format=lambda x: f"{x:.6f}"))

if __name__ == "__main__":
    main()
