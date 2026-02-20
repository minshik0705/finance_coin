#!/usr/bin/env python3
"""
Quick, orderly IsolationForest anomaly detection for Binance klines (5m).
- Fit on 2020-2023 only; score all years.
- Set alert rate via train-score quantiles (default: 0.5% anomalies).
- Save: model (.joblib), anomalies (.parquet), plot (.png)
"""

import os
from pathlib import Path
import argparse
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline
import joblib

os.environ.setdefault("MPLBACKEND", "Agg")  # safe for servers

# -----------------------
# Args
# -----------------------
def get_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="data/2020-2025.csv", help="Input CSV path")
    p.add_argument("--outdir", default=".", help="Output directory")
    p.add_argument("--strong_q", type=float, default=0.001, help="Strong anomaly quantile (train)")
    p.add_argument("--anom_q", type=float, default=0.005, help="Overall anomaly quantile (train)")
    p.add_argument("--contam", type=float, default=0.003, help="IF contamination (training prior)")
    return p.parse_args()

# -----------------------
# Feature engineering
# -----------------------
def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # numeric
    num_cols = ["Open", "High", "Low", "Close", "Volume"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # timestamp
    df["ts"] = pd.to_datetime(df["Open time"], unit="ms", utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    # aliases
    open_, high, low, close, vol = df["Open"], df["High"], df["Low"], df["Close"], df["Volume"]

    # returns (log)
    df["logret"] = np.log(close).diff()

    # candle shape (normalize by price; guard zero)
    denom = close.replace(0, np.nan)
    df["range"] = (high - low) / denom
    df["body"] = (close - open_) / denom
    df["upper_wick"] = (high - np.maximum(open_, close)) / denom
    df["lower_wick"] = (np.minimum(open_, close) - low) / denom

    # volume features (robust)
    df["logvol"] = np.log1p(vol)  # avoids -inf at 0
    df["dlogvol"] = df["logvol"].diff()

    # rolling windows for 5m bars: 12=~1h, 288=~1d
    df["vol_1h"] = df["logret"].rolling(12, min_periods=12).std()
    df["vol_1d"] = df["logret"].rolling(288, min_periods=288).std()

    # classic z of volume (optionally replace with robust z if needed)
    vmean = vol.rolling(288, min_periods=288).mean()
    vstd = vol.rolling(288, min_periods=288).std()
    df["vol_z_1d"] = (vol - vmean) / vstd

    # final feature set
    feat_cols = [
        "logret", "range", "body", "upper_wick", "lower_wick",
        "dlogvol", "vol_1h", "vol_1d", "vol_z_1d"
    ]
    X = df[feat_cols].replace([np.inf, -np.inf], np.nan).dropna()
    df_model = df.loc[X.index].copy()

    return df_model, X, feat_cols

# -----------------------
# Reason strings
# -----------------------
def interpret_row(row):
    reasons = []
    if abs(row.get("logret", 0)) > 0.02:
        reasons.append(f"Large price move: {row['logret']*100:.2f}%")
    if row.get("vol_z_1d", 0) > 3:
        reasons.append(f"Volume spike: {row['vol_z_1d']:.1f} std devs")
    if row.get("range", 0) > 0.05:
        reasons.append(f"Wide range: {row['range']*100:.1f}%")
    if row.get("vol_1h", 0) > 2 * row.get("vol_1d", 1e9):
        reasons.append("Volatility spike")
    return "; ".join(reasons) if reasons else "Unknown"

# -----------------------
# Train, score, export
# -----------------------
def main():
    args = get_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Load + features
    df = pd.read_csv(args.csv)
    df_model, X, feat_cols = build_features(df)

    # 2) Time split
    train_cut = pd.Timestamp("2024-01-01", tz="UTC")
    val_cut   = pd.Timestamp("2025-01-01", tz="UTC")
    train_m = df_model["ts"] < train_cut
    val_m   = (df_model["ts"] >= train_cut) & (df_model["ts"] < val_cut)
    test_m  = df_model["ts"] >= val_cut

    X_train = X[train_m]
    X_val   = X[val_m]
    X_test  = X[test_m]

    # 3) Model (fit once on train)
    model = Pipeline(steps=[
        ("scaler", RobustScaler()),
        ("iso", IsolationForest(
            n_estimators=400,
            contamination=args.contam,
            max_samples="auto",
            random_state=42,
            n_jobs=-1
        ))
    ])
    model.fit(X_train)

    # 4) Score all splits
    df_model.loc[train_m, "anomaly_score"] = model.decision_function(X_train)
    df_model.loc[val_m,   "anomaly_score"] = model.decision_function(X_val)
    df_model.loc[test_m,  "anomaly_score"] = model.decision_function(X_test)

    # Label by IF sign (for reference)
    df_model.loc[train_m, "anomaly_label"] = model.predict(X_train)
    df_model.loc[val_m,   "anomaly_label"] = model.predict(X_val)
    df_model.loc[test_m,  "anomaly_label"] = model.predict(X_test)

    # 5) Quantile thresholds from TRAIN only
    train_scores = df_model.loc[train_m, "anomaly_score"].to_numpy()
    thr_overall  = np.quantile(train_scores, args.anom_q)
    thr_strong   = np.quantile(train_scores, args.strong_q)

    df_model["is_anom"] = df_model["anomaly_score"] <= thr_overall
    strong = df_model["anomaly_score"] <= thr_strong

    # Human-readable reason strings for anomalies
    anoms = df_model[df_model["is_anom"]].copy()
    anoms["reason"] = anoms.apply(interpret_row, axis=1)

    # 6) Save artifacts
    # 6a) model + meta
    meta = {
        "feature_cols": feat_cols,
        "train_range": [
            str(df_model.loc[train_m, "ts"].min()),
            str(df_model.loc[train_m, "ts"].max())
        ],
        "contamination": args.contam,
        "threshold_overall": float(thr_overall),
        "threshold_strong": float(thr_strong),
        "rows_train": int(train_m.sum())
    }
    joblib.dump({"model": model, "meta": meta}, outdir / "btc_isoforest.joblib")

    # 6b) anomalies parquet
    out_cols = ["ts","Open","High","Low","Close","Volume",
                "anomaly_score","anomaly_label","is_anom"]
    anoms[out_cols + ["reason"]].to_parquet(outdir / "anomalies.parquet", index=False)

    # 6c) plot
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 8), sharex=True)

        ax1.plot(df_model["ts"], df_model["Close"], lw=0.8, alpha=0.7, label="BTC Price")
        s = df_model[strong]
        ax1.scatter(s["ts"], s["Close"], s=18, color="crimson", alpha=0.9,
                    label=f"Strong anomalies ({len(s)})", zorder=5)
        ax1.set_ylabel("Price (USDT)")
        ax1.legend()
        ax1.grid(alpha=0.3)

        ax2.fill_between(df_model["ts"].values, 0, df_model["Volume"].values,
                         alpha=0.3, label="Volume")
        ax2.set_ylabel("Volume")
        ax2.set_xlabel("Time")
        ax2.legend()
        ax2.grid(alpha=0.3)
        ax2.xaxis.set_major_locator(mdates.YearLocator())
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

        plt.tight_layout()
        out_png = outdir / "anomalies_visualization.png"
        plt.savefig(out_png, dpi=150)
        plt.close()
        print(f"Saved plot → {out_png}")
    except Exception as e:
        print(f"Plot skipped: {e}")

    # 7) Console summary
    print(f"Total scored: {len(df_model):,}")
    print(f"Anomalies (<= q{args.anom_q:.3%}): {anoms.shape[0]:,}")
    print(anoms[["ts","Close","Volume","anomaly_score","reason"]]
          .nsmallest(10, "anomaly_score")
          .to_string(index=False))
    
    dfc = df_model[df_model["anomaly_score"] <= meta["threshold_strong"]]
    print(dfc.groupby(dfc["ts"].dt.year).size())

    print(dfc.nsmallest(10, "anomaly_score")[["ts","Close","Volume","anomaly_score"]])


if __name__ == "__main__":
    main()
