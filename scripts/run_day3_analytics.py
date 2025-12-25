import sys
from pathlib import Path
import logging

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.transforms import parse_datetime, add_time_parts, flag_outliers_iqr
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


def main() -> None:
  
    log.info("Loading Day 2 artifacts")
    orders = pd.read_parquet("data/processed/orders_clean.parquet")
    users = pd.read_parquet("data/processed/users.parquet")

   
    log.info("Parsing created_at + adding time parts")
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    
    log.info("Flagging amount outliers")
    orders["amount__outlier"] = flag_outliers_iqr(orders["amount"])

    # 4) Join with users (Day 3 Task 4)
    log.info("Joining orders with users")
    joined = safe_left_join(orders, users)
        # --- Task 7: mini summary table (revenue + count by country) ---
    summary = (
        joined.groupby("country", dropna=False)
        .agg(n=("order_id", "size"), revenue=("amount", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )

    print("\n=== Revenue by country ===")
    print(summary.to_string(index=False))

 
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)


   
    out_path = Path("data/processed/orders_analytics.parquet")
    joined.to_parquet(out_path, index=False)

    log.info("Wrote analytics dataset: %s", out_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
