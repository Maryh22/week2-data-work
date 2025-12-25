import sys
from pathlib import Path
import logging

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_orders_schema
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
)

log = logging.getLogger(__name__)


def main() -> None:
    p = make_paths(ROOT)

    log.info("Loading raw data")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")

    
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")

   
    orders = enforce_orders_schema(orders_raw)

   
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

  
    missing_count = orders.isna().sum()
    missing_pct = (missing_count / len(orders)) if len(orders) else 0
    report = pd.DataFrame({"missing_count": missing_count, "missing_pct": missing_pct}).sort_values(
        "missing_count", ascending=False
    )
    report.to_csv(reports_dir / "missingness_orders.csv", index=True)

    
    orders["status_clean"] = (
        orders["status"].astype("string").str.strip().str.lower().replace({"refunded": "refund"})
    )

  
    orders["amount__isna"] = orders["amount"].isna()
    orders["quantity__isna"] = orders["quantity"].isna()

  
    assert_unique_key(users, "user_id")
    assert_unique_key(orders, "order_id")

   
    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=0, name="quantity")

   
    write_parquet(orders, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    
s = orders["amount"].dropna()
print(s.quantile([0.5, 0.9, 0.99]))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
