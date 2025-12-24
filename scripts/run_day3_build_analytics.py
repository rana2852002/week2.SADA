from pathlib import Path
import pandas as pd

from bootcamp_data.config import make_paths
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
   
)
from bootcamp_data.joins import safe_left_join


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    p = make_paths(ROOT)

 
    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users  = pd.read_parquet(p.processed / "users.parquet")

  
    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])

    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    print(
        "missing created_at after parse:",
        int(orders_t["created_at"].isna().sum()),
        "/",
        len(orders_t),
    )

    
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    print("rows after join:", len(joined))

    match_rate = 1.0 - float(joined["country"].isna().mean())
    print("country match rate:", round(match_rate, 3))

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))


    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)

    print("wrote:", out_path)


if __name__ == "__main__":
    main()
