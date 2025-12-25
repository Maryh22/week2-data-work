from pathlib import Path
import pandas as pd

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_orders_schema, enforce_users_schema


def main() -> None:
    root = Path(".").resolve()
    p = make_paths(root)

    # 1) Extract (read raw)
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users_raw = read_users_csv(p.raw / "users.csv")

    # 2) Transform (clean)
    orders = enforce_orders_schema(orders_raw)
    users = enforce_users_schema(users_raw)

    # 3) Load (write processed)
    write_parquet(orders, p.processed / "orders.parquet")
    write_parquet(users, p.processed / "users.parquet")

    print("Done: wrote data/processed/orders.parquet and data/processed/users.parquet")


if __name__ == "__main__":
    main()
