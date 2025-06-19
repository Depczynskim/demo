import glob
import json
import math
from pathlib import Path

import pandas as pd


def extract_param(json_str: str | None, key: str):
    """Return value of GA4 event parameter *key* from the event_params_json column."""
    if json_str is None or pd.isna(json_str):
        return None
    try:
        params = json.loads(json_str)
        for p in params:
            if p.get("key") == key:
                v = p.get("value", {})
                return (
                    v.get("int_value")
                    or v.get("float_value")
                    or v.get("double_value")
                    or v.get("string_value")
                )
    except Exception:
        return None
    return None


def load_ga4_events(base_path: str = "data_repo/ga4/analytics_events_final") -> pd.DataFrame:
    """Load all GA4 parquet files under *base_path* into one dataframe (selected cols)."""
    paths = glob.glob(f"{base_path}/report_month=*/data_*.parquet")
    if not paths:
        raise FileNotFoundError("No GA4 parquet files found – run extractor first.")
    parts = []
    cols = [
        "event_name",
        "event_params_json",
        "user_pseudo_id",
        "device_category",
        "geo_country",
        "traffic_source",
        "traffic_medium",
    ]
    for p in paths:
        try:
            parts.append(pd.read_parquet(p, columns=cols))
        except Exception as e:
            print(f"⚠️  Failed reading {p}: {e}")
    df = pd.concat(parts, ignore_index=True)
    return df


def session_level_df(events_df: pd.DataFrame) -> pd.DataFrame:
    """Return session-level dataframe with derived behavioural features."""
    events_df = events_df.copy()
    events_df["ga_session_id"] = events_df["event_params_json"].apply(lambda x: extract_param(x, "ga_session_id"))
    events_df = events_df[events_df["ga_session_id"].notna()].copy()

    sess = events_df.groupby("ga_session_id").agg(
        user_pseudo_id=("user_pseudo_id", "first"),
        geo_country=("geo_country", "first"),
        device_category=("device_category", "first"),
        traffic_source=("traffic_source", "first"),
        traffic_medium=("traffic_medium", "first"),
        n_pageviews=("event_name", lambda x: (x == "page_view").sum()),
        n_search=("event_name", lambda x: (x == "search").sum()),
        n_faq=("event_name", lambda x: (x == "faq_interaction").sum()),
        n_gallery=("event_name", lambda x: (x == "photo_gallery_click").sum()),
        purchase=("event_name", lambda x: (x == "purchase").any()),
        level5_intent=("event_name", lambda x: x.isin(["add_to_cart", "form_start", "view_cart", "begin_checkout"]).any()),
    ).reset_index()
    sess["high_intent"] = sess["level5_intent"].astype(int)
    sess["buyer"] = sess["purchase"].astype(int)
    return sess


def signal_lifts(sess: pd.DataFrame):
    print("Total sessions:", len(sess))
    print("High-intent sessions (L5):", sess["high_intent"].sum())
    print("Purchase sessions:", sess["buyer"].sum())
    print("\nSignal lift (flag => >= threshold):")
    signals = [
        ("n_pageviews", "Pageviews ≥3", 3),
        ("n_search", "On-site search", 1),
        ("n_faq", "FAQ click", 1),
        ("n_gallery", "Gallery click", 1),
    ]
    for col, label, thresh in signals:
        flag = sess[col] >= thresh
        pct_buyers = flag[sess["high_intent"] == 1].mean() if sess["high_intent"].sum() else 0.0
        pct_others = flag[sess["high_intent"] == 0].mean()
        lift = (pct_buyers / pct_others) if pct_others else math.nan
        print(f"{label:15s}: buyers={pct_buyers:6.1%} | others={pct_others:6.1%} | lift={lift:4.1f}×")


if __name__ == "__main__":
    df_events = load_ga4_events()
    df_sessions = session_level_df(df_events)
    signal_lifts(df_sessions)

    # ------------------------------------------------------------------
    # Predictive modelling – logistic regression to rank feature signals
    # ------------------------------------------------------------------
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler
        import numpy as np
        import warnings

        # Features
        cat_cols = ["device_category", "geo_country", "traffic_source", "traffic_medium"]
        num_cols = ["n_pageviews", "n_search", "n_faq", "n_gallery"]

        # Simplify high-cardinality categoricals (keep top 4)
        for col in cat_cols:
            top_vals = df_sessions[col].value_counts().nlargest(4).index
            df_sessions[col] = df_sessions[col].where(df_sessions[col].isin(top_vals), other="other")

        X_cat = pd.get_dummies(df_sessions[cat_cols], prefix=cat_cols, dummy_na=True)
        X_num = df_sessions[num_cols].astype(float).fillna(0)
        X = pd.concat([X_num, X_cat], axis=1)
        y = df_sessions["high_intent"].astype(int)

        if y.sum() < 30:
            print("\nNot enough positive examples for modelling – skipping logreg.")
        else:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.3, random_state=42, stratify=y
            )

            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train[num_cols])
            X_test_scaled = scaler.transform(X_test[num_cols])

            # Replace numeric columns with scaled, keep dummy columns as-is
            X_train_comb = np.hstack([X_train_scaled, X_train[X_cat.columns].values])
            X_test_comb = np.hstack([X_test_scaled, X_test[X_cat.columns].values])

            # Build class-balanced logistic regression (liblinear handles small sets well)
            model = LogisticRegression(class_weight="balanced", max_iter=1000, solver="liblinear")
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                model.fit(X_train_comb, y_train)

            from sklearn.metrics import roc_auc_score

            y_prob = model.predict_proba(X_test_comb)[:, 1]
            auc = roc_auc_score(y_test, y_prob)
            print(f"\nLogistic regression AUC: {auc:.3f}\n")

            # Feature importance
            feature_names = list(num_cols) + list(X_cat.columns)
            coefs = pd.Series(model.coef_[0], index=feature_names)
            top = coefs.abs().sort_values(ascending=False).head(15)
            print("Top predictive features (coefficients, sign indicates direction):")
            for name, val in top.items():
                direction = "↑" if val > 0 else "↓"
                print(f"{name:30s} {direction} {val:+.3f}")
    except Exception as e:
        print("⚠️  Modelling step failed:", e) 