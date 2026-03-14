import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

engine = create_engine("mysql+mysqlconnector://root:Zyy0726%40zyy@127.0.0.1:3306/uber_ncr")
df = pd.read_sql("SELECT * FROM v_stage2_model_base", engine)

# Winsorize VTAT
q1, q99 = df["avg_vtat"].quantile([0.01, 0.99])
df["avg_vtat_w"] = df["avg_vtat"].clip(q1, q99)

# 分段特征（替代线性VTAT）
bins = [0, 2, 4, 6, 8, 10, 15, 30]
df["vtat_bin"] = pd.cut(df["avg_vtat_w"], bins=bins, right=False)

X = df[["vtat_bin", "hour_of_day", "dow", "vehicle_type", "pickup_location"]].copy()
y = df["is_cancel"].astype(int)

pre = ColumnTransformer([
    ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"),
     ["vtat_bin", "vehicle_type", "pickup_location"]),
    ("num", "passthrough", ["hour_of_day", "dow"])
])

clf = Pipeline([
    ("pre", pre),
    ("lr", LogisticRegression(max_iter=4000))
])

Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
clf.fit(Xtr, ytr)
auc = roc_auc_score(yte, clf.predict_proba(Xte)[:, 1])
print("AUC_piecewise =", round(auc, 4))

# 输出VTAT分段Odds Ratio（相对基准箱）
ohe = clf.named_steps["pre"].named_transformers_["cat"]
feature_names = ohe.get_feature_names_out(["vtat_bin", "vehicle_type", "pickup_location"])
coef = clf.named_steps["lr"].coef_[0]
coef_map = dict(zip(list(feature_names) + ["hour_of_day", "dow"], coef))

print("\nVTAT bin odds ratio:")
for k, v in coef_map.items():
    if k.startswith("vtat_bin_"):
        print(k, "OR=", round(np.exp(v), 3))