from __future__ import annotations

import json
import logging
import inspect
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from bootcamp_data.io import read_orders_csv, read_users_csv

# These modules/functions must exist in your project
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import (
    enforce_orders_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    clip_outliers_iqr,
    flag_outliers_iqr,
)
from bootcamp_data.joins import safe_left_join

# Try to import write_parquet; if not available, use a fallback
try:
    from bootcamp_data.io import write_parquet  # type: ignore
except Exception:  # pragma: no cover
    write_parquet = None  # type: ignore

log = logging.getLogger(__name__)


# ----------------------------
# Task 1 — Config
# ----------------------------
@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


# ----------------------------
# Task 2 — Load inputs
# ----------------------------
def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


# ----------------------------
# Helpers (to adapt to your local function signatures)
# ----------------------------
def _call_safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    key: str,
    validate: str = "many_to_one",
    suffixes: tuple[str, str] = ("", "_user"),
) -> pd.DataFrame:
    """
    Call safe_left_join even if the function signature differs across versions.
    It may accept: on=..., key=..., left_on/right_on, etc.
    """
    sig = inspect.signature(safe_left_join)
    params = sig.parameters

    kwargs: dict[str, object] = {}

    # Most common patterns:
    if "on" in params:
        kwargs["on"] = key
    elif "key" in params:
        kwargs["key"] = key
    else:
        # Fall back to left_on/right_on if present
        if "left_on" in params and "right_on" in params:
            kwargs["left_on"] = key
            kwargs["right_on"] = key

    if "validate" in params:
        kwargs["validate"] = validate
    if "suffixes" in params:
        kwargs["suffixes"] = suffixes

    return safe_left_join(left, right, **kwargs)  # type: ignore[arg-type]

def _add_outlier_flag_iqr(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # flag_outliers_iqr expects a Series and returns a boolean/0-1 Series
    out_s = flag_outliers_iqr(df[col])  # pass only the column
    if isinstance(out_s, pd.Series):
        return df.assign(**{f"{col}_outlier": out_s.astype(int)})
    return df

    sig = inspect.signature(flag_outliers_iqr)
    params = list(sig.parameters.keys())

    # If it accepts a DataFrame
    if len(params) >= 2:
        # try keyword 'col'
        if "col" in sig.parameters:
            out = flag_outliers_iqr(df, col=col)  # type: ignore[misc]
        else:
            out = flag_outliers_iqr(df, col)  # type: ignore[misc]

        # If it returns a DataFrame, use it
        if isinstance(out, pd.DataFrame):
            return out

        # If it returns a Series, attach it
        if isinstance(out, pd.Series):
            return df.assign(**{f"{col}_outlier": out.astype(int)})
        return df

    # Otherwise assume it accepts a Series and returns a Series
    out_s = flag_outliers_iqr(df[col])  # type: ignore[misc]
    if isinstance(out_s, pd.Series):
        return df.assign(**{f"{col}_outlier": out_s.astype(int)})
    return df


def _write_parquet_fallback(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


# ----------------------------
# Task 3 — Transform
# ----------------------------
def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    # checks: required columns, non-empty
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

    # assert uniqueness on users
    assert_unique_key(users, "user_id")

    # schema enforcement
    orders = enforce_orders_schema(orders_raw)

    # status normalization + mapping
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))

    # missing flags
    orders = orders.pipe(add_missing_flags, cols=["amount", "quantity"])

    # parse datetime + time parts
    orders = (
        orders.pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    # safe left join orders → users
    joined = _call_safe_left_join(
        orders,
        users,
        key="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    # post-join row count sanity
    assert len(joined) == len(orders), "Row count changed (join explosion?)"

    # winsorize amount (IQR clip) + outlier flag
    joined = joined.assign(amount_winsor=clip_outliers_iqr(joined["amount"]))
    joined = _add_outlier_flag_iqr(joined, "amount")

    return joined


# ----------------------------
# Task 4 — Outputs + Run metadata
# ----------------------------
def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    if write_parquet is not None:
        write_parquet(users, cfg.out_users)       # type: ignore[misc]
        write_parquet(analytics, cfg.out_analytics)  # type: ignore[misc]
    else:
        _write_parquet_fallback(users, cfg.out_users)
        _write_parquet_fallback(analytics, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs")
    orders_raw, users = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users))
    analytics = transform(orders_raw, users)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics, users, cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics=analytics)
