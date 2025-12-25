import re
import pandas as pd


def enforce_orders_schema(df):
    
    df = df.copy()

    df["order_id"] = df["order_id"].astype(str)
    df["user_id"] = df["user_id"].astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["status"] = df["status"].astype(str).str.strip().str.lower()

    return df


def enforce_users_schema(df):
    """تنظيف جدول users."""
    df = df.copy()

    df["user_id"] = df["user_id"].astype(str)
    df["country"] = df["country"].astype(str).str.strip().str.upper()
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    return df


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return (df.isna().sum() .rename("n_missing") .to_frame() .assign(p_missing=lambda t: t["n_missing"] / len(df)) .sort_values("p_missing", ascending=False)
            #ترج عدد المسنق
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string").str.strip() .str.casefold() .str.replace(_ws, " ", regex=True)
    )


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))


def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    return (
        df.sort_values(ts_col).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
    )

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})
#داله تخذ DataFrem تحول عامود التواريخ الي نوعه سترنق الى داتاتايم

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date, year=ts.dt.year,month=ts.dt.to_period("M").astype("string"),dow=ts.dt.day_name(),hour=ts.dt.hour,
    )
# يسير عندي كولمز لكل جزء في التاريخ 

def flag_outliers_iqr(s: pd.Series, *, k: float = 1.5) -> pd.Series:
    x = s.dropna()
    if x.empty:
        
        return pd.Series([False] * len(s), index=s.index)

    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1

    lo = q1 - k * iqr
    hi = q3 + k * iqr

    return (s < lo) | (s > hi)


def clip_outliers_iqr(s: pd.Series, *, k: float = 1.5) -> pd.Series:
   
    x = s.dropna()
    if x.empty:
        return s

    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1

    lo = q1 - k * iqr
    hi = q3 + k * iqr

    return s.clip(lower=lo, upper=hi)
