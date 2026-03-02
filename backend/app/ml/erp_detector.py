"""
ERP/SAP Invoice Anomaly Detection Engine — Phase 3
====================================================
Detection rules:
  EXISTING:
  1. Duplicate Invoices
  2. Quantity-Price Mismatch
  3. Round Number Fraud
  4. Vendor Behavior Anomaly
  5. Statistical Outliers (Isolation Forest)

  NEW IN PHASE 3:
  6. Contract Deviation       — vendor charged more than agreed contract price
  7. Approval Bypass          — high-value invoice with no approver or unauthorized approver
  8. Duplicate Vendor+Amount  — same vendor & amount, different invoice ID (sneaky duplicate)
  9. Weekend Invoice          — invoice dated on a weekend (common fraud pattern)
  10. New Vendor High Value   — brand new vendor submitting unusually large invoice
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ERPAnomalyDetector:

    def __init__(
        self,
        zscore_threshold: float = 3.0,
        contamination: float = 0.05,
        contract_rules: Optional[Dict[str, float]] = None,
        approved_approvers: Optional[List[str]] = None,
        high_value_threshold: Optional[float] = None
    ):
        self.zscore_threshold = zscore_threshold
        self.contamination = contamination
        self.contract_rules = contract_rules or {}
        self.approved_approvers = [a.strip().lower() for a in approved_approvers] if approved_approvers else []
        self.high_value_threshold_override = high_value_threshold
        self.isolation_forest = IsolationForest(contamination=contamination, random_state=42, n_estimators=100)
        self.scaler = StandardScaler()

    def analyze(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        anomalies = []
        df = df.copy()
        df = self._normalize_columns(df)
        logger.info(f"Analyzing {len(df)} invoice records with Phase 3 engine...")

        anomalies.extend(self._detect_duplicate_invoices(df))
        anomalies.extend(self._detect_quantity_price_mismatch(df))
        anomalies.extend(self._detect_round_number_fraud(df))
        anomalies.extend(self._detect_vendor_behavior(df))
        anomalies.extend(self._detect_statistical_outliers(df))
        anomalies.extend(self._detect_contract_deviation(df))
        anomalies.extend(self._detect_approval_bypass(df))
        anomalies.extend(self._detect_duplicate_vendor_amount(df))
        anomalies.extend(self._detect_weekend_invoices(df))
        anomalies.extend(self._detect_new_vendor_high_value(df))

        seen = {}
        for a in anomalies:
            idx = a["row_index"]
            if idx not in seen or a["anomaly_score"] > seen[idx]["anomaly_score"]:
                seen[idx] = a

        result = list(seen.values())
        result.sort(key=lambda x: x["anomaly_score"], reverse=True)
        logger.info(f"Phase 3 engine detected {len(result)} anomalies")
        return result

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_map = {
            "invoice_number": "invoice_id", "invoice_no": "invoice_id", "inv_no": "invoice_id", "invoice": "invoice_id",
            "invoice_amount": "amount", "total_amount": "amount", "bill_amount": "amount", "net_amount": "amount", "value": "amount",
            "vendor_name": "vendor", "supplier": "vendor", "supplier_name": "vendor", "party_name": "vendor",
            "qty": "quantity", "units": "quantity",
            "rate": "unit_price", "price_per_unit": "unit_price",
            "approved_by": "approved_by", "approver": "approved_by",
            "invoice_date": "invoice_date", "date": "invoice_date", "bill_date": "invoice_date",
        }
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df.rename(columns={k: v for k, v in column_map.items() if k in df.columns}, inplace=True)
        for col in ["amount", "quantity", "unit_price"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "invoice_date" in df.columns:
            df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
        return df

    def _detect_duplicate_invoices(self, df):
        if "invoice_id" not in df.columns:
            return []
        duplicates = df[df.duplicated(subset=["invoice_id"], keep=False)]
        results = []
        for idx, row in duplicates.iterrows():
            count = (df["invoice_id"] == row["invoice_id"]).sum()
            results.append(self._build_anomaly(idx, row.to_dict(), "duplicate_invoice", 90.0, "high",
                f"Invoice ID '{row.get('invoice_id')}' appears {count} times. Possible duplicate submission — risk of double payment.",
                ["invoice_id"]))
        return results

    def _detect_quantity_price_mismatch(self, df):
        if not all(c in df.columns for c in ["quantity", "unit_price", "amount"]):
            return []
        results = []
        for idx, row in df.iterrows():
            try:
                expected = round(row["quantity"] * row["unit_price"], 2)
                actual = round(row["amount"], 2)
                if expected == 0:
                    continue
                pct_diff = abs(expected - actual) / expected * 100
                if pct_diff > 5:
                    results.append(self._build_anomaly(idx, row.to_dict(), "quantity_price_mismatch",
                        min(50 + pct_diff, 95), "high" if pct_diff > 20 else "medium",
                        f"Qty ({row['quantity']}) × Unit Price ({row['unit_price']}) = {expected}, but Invoice Amount = {actual}. Discrepancy of {pct_diff:.1f}%.",
                        ["quantity", "unit_price", "amount"]))
            except Exception:
                continue
        return results

    def _detect_round_number_fraud(self, df):
        if "amount" not in df.columns:
            return []
        results = []
        median_amount = df["amount"].median()
        threshold = median_amount * 3
        for idx, row in df.iterrows():
            try:
                amount = row["amount"]
                if pd.isna(amount) or amount <= 0:
                    continue
                if (amount % 1000 == 0) and (amount > threshold):
                    results.append(self._build_anomaly(idx, row.to_dict(), "round_number_amount", 55.0, "medium",
                        f"Invoice amount of {amount:,.0f} is a perfectly round number and significantly above the typical value ({median_amount:,.0f}). Round numbers at high values are a known fraud indicator.",
                        ["amount"]))
            except Exception:
                continue
        return results

    def _detect_vendor_behavior(self, df):
        if "vendor" not in df.columns or "amount" not in df.columns:
            return []
        results = []
        vendor_stats = df.groupby("vendor")["amount"].agg(["mean", "std", "count"]).reset_index()
        for idx, row in df.iterrows():
            try:
                vendor = row.get("vendor")
                amount = row.get("amount")
                if pd.isna(vendor) or pd.isna(amount):
                    continue
                stats = vendor_stats[vendor_stats["vendor"] == vendor]
                if stats.empty or stats["count"].values[0] < 3:
                    continue
                mean = stats["mean"].values[0]
                std = stats["std"].values[0]
                if std == 0 or pd.isna(std):
                    continue
                z = (amount - mean) / std
                if z > self.zscore_threshold:
                    results.append(self._build_anomaly(idx, row.to_dict(), "vendor_amount_spike",
                        min(60 + z * 5, 95), "high" if z > 4 else "medium",
                        f"Vendor '{vendor}' typically invoices around {mean:,.0f} (±{std:,.0f}). This invoice of {amount:,.0f} is {z:.1f} standard deviations above their norm.",
                        ["vendor", "amount"]))
            except Exception:
                continue
        return results

    def _detect_statistical_outliers(self, df):
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) == 0 or len(df) < 10:
            return []
        try:
            ml_df = df[numeric_cols].fillna(df[numeric_cols].median())
            scaled = self.scaler.fit_transform(ml_df)
            predictions = self.isolation_forest.fit_predict(scaled)
            scores = self.isolation_forest.score_samples(scaled)
            norm_scores = 1 - (scores - scores.min()) / (scores.max() - scores.min() + 1e-10)
            norm_scores = norm_scores * 100
            results = []
            for i, (pred, score) in enumerate(zip(predictions, norm_scores)):
                if pred == -1:
                    row = df.iloc[i]
                    results.append(self._build_anomaly(i, row.to_dict(), "statistical_outlier",
                        float(score), self._score_to_severity(float(score)),
                        f"This invoice is statistically unusual across multiple dimensions. The ML model flagged it as an outlier (risk score: {score:.0f}/100).",
                        numeric_cols))
            return results
        except Exception as e:
            logger.error(f"Isolation Forest failed: {e}")
            return []

    def _detect_contract_deviation(self, df):
        if "vendor" not in df.columns or "amount" not in df.columns:
            return []
        results = []
        if self.contract_rules:
            for idx, row in df.iterrows():
                try:
                    vendor = row.get("vendor")
                    amount = row.get("amount")
                    if pd.isna(vendor) or pd.isna(amount):
                        continue
                    limit = next((v for k, v in self.contract_rules.items() if k.lower() == str(vendor).lower()), None)
                    if limit and amount > limit:
                        pct_over = (amount - limit) / limit * 100
                        results.append(self._build_anomaly(idx, row.to_dict(), "contract_deviation",
                            min(70 + pct_over * 0.5, 97), "high" if pct_over > 30 else "medium",
                            f"Vendor '{vendor}' has a contract limit of {limit:,.0f} but invoiced {amount:,.0f} — that's {pct_over:.1f}% over the agreed limit. This may indicate unauthorized charges.",
                            ["vendor", "amount"]))
                except Exception:
                    continue
        else:
            vendor_count = df.groupby("vendor")["amount"].count()
            for idx, row in df.iterrows():
                try:
                    vendor = row.get("vendor")
                    amount = row.get("amount")
                    if pd.isna(vendor) or pd.isna(amount):
                        continue
                    if vendor_count.get(vendor, 0) < 3:
                        continue
                    other_amounts = df[(df["vendor"] == vendor) & (df.index != idx)]["amount"]
                    if other_amounts.empty:
                        continue
                    historical_max = other_amounts.max()
                    if amount > historical_max * 1.5:
                        pct_over = (amount - historical_max) / historical_max * 100
                        results.append(self._build_anomaly(idx, row.to_dict(), "contract_deviation",
                            min(65 + pct_over * 0.3, 92), "high" if pct_over > 100 else "medium",
                            f"Vendor '{vendor}' has never invoiced more than {historical_max:,.0f} historically, but this invoice is {amount:,.0f} — {pct_over:.1f}% above their highest ever amount. Possible contract breach.",
                            ["vendor", "amount"]))
                except Exception:
                    continue
        return results

    def _detect_approval_bypass(self, df):
        if "amount" not in df.columns:
            return []
        results = []
        median_amount = df["amount"].median()
        threshold = self.high_value_threshold_override or (median_amount * 5)
        has_approver_col = "approved_by" in df.columns
        for idx, row in df.iterrows():
            try:
                amount = row.get("amount")
                if pd.isna(amount) or amount < threshold:
                    continue
                approved_by = row.get("approved_by") if has_approver_col else None
                is_missing = pd.isna(approved_by) or str(approved_by).strip() == ""
                is_unauthorized = (self.approved_approvers and not is_missing and
                                   str(approved_by).strip().lower() not in self.approved_approvers)
                if is_missing:
                    results.append(self._build_anomaly(idx, row.to_dict(), "approval_bypass", 85.0, "high",
                        f"Invoice for {amount:,.0f} has NO approver recorded. High-value invoices above {threshold:,.0f} require approval. This may indicate a control bypass.",
                        ["amount", "approved_by"]))
                elif is_unauthorized:
                    results.append(self._build_anomaly(idx, row.to_dict(), "approval_bypass", 75.0, "high",
                        f"Invoice for {amount:,.0f} was approved by '{approved_by}' who is not in the authorized approvers list. Possible unauthorized approval.",
                        ["amount", "approved_by"]))
            except Exception:
                continue
        return results

    def _detect_duplicate_vendor_amount(self, df):
        if "vendor" not in df.columns or "amount" not in df.columns:
            return []
        results = []
        groups = df.groupby(["vendor", "amount"])
        for (vendor, amount), group in groups:
            if len(group) < 2:
                continue
            if "invoice_id" in df.columns and group["invoice_id"].nunique() < 2:
                continue
            for idx, row in group.iterrows():
                results.append(self._build_anomaly(idx, row.to_dict(), "duplicate_vendor_amount", 78.0, "high",
                    f"Vendor '{vendor}' has submitted {len(group)} invoices all for exactly {amount:,.0f} with different invoice IDs. This pattern suggests a disguised duplicate payment attempt.",
                    ["vendor", "amount", "invoice_id"]))
        return results

    def _detect_weekend_invoices(self, df):
        if "invoice_date" not in df.columns or "amount" not in df.columns:
            return []
        results = []
        median_amount = df["amount"].median()
        for idx, row in df.iterrows():
            try:
                date = row.get("invoice_date")
                amount = row.get("amount")
                if pd.isna(date) or pd.isna(amount) or amount < median_amount:
                    continue
                day_of_week = pd.Timestamp(date).dayofweek
                if day_of_week >= 5:
                    day_name = "Saturday" if day_of_week == 5 else "Sunday"
                    results.append(self._build_anomaly(idx, row.to_dict(), "weekend_invoice", 60.0, "medium",
                        f"This invoice is dated on a {day_name} ({pd.Timestamp(date).strftime('%Y-%m-%d')}). Business invoices are rarely issued on weekends. This may indicate backdating or fraudulent creation.",
                        ["invoice_date", "amount"]))
            except Exception:
                continue
        return results

    def _detect_new_vendor_high_value(self, df):
        if "vendor" not in df.columns or "amount" not in df.columns:
            return []
        results = []
        vendor_counts = df["vendor"].value_counts()
        median_amount = df["amount"].median()
        threshold = median_amount * 4
        for idx, row in df.iterrows():
            try:
                vendor = row.get("vendor")
                amount = row.get("amount")
                if pd.isna(vendor) or pd.isna(amount):
                    continue
                if vendor_counts.get(vendor, 0) == 1 and amount > threshold:
                    results.append(self._build_anomaly(idx, row.to_dict(), "new_vendor_high_value", 72.0, "high",
                        f"Vendor '{vendor}' appears for the first time in this dataset with a high-value invoice of {amount:,.0f} (threshold: {threshold:,.0f}). New vendors submitting large invoices immediately is a known fraud pattern — verify this vendor exists and is approved.",
                        ["vendor", "amount"]))
            except Exception:
                continue
        return results

    def _build_anomaly(self, row_index, row_data, anomaly_type, score, severity, explanation, features):
        clean_data = {}
        for k, v in row_data.items():
            if isinstance(v, pd.Timestamp):
                clean_data[k] = str(v.date())
            elif isinstance(v, float) and pd.isna(v):
                clean_data[k] = None
            else:
                clean_data[k] = v
        return {
            "row_index": int(row_index),
            "anomaly_type": anomaly_type,
            "anomaly_score": round(min(max(score, 0), 100), 2),
            "severity": severity,
            "explanation": explanation,
            "features_flagged": features,
            "record_data": clean_data
        }

    def _score_to_severity(self, score: float) -> str:
        if score >= 75:
            return "high"
        elif score >= 50:
            return "medium"
        return "low"