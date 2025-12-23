import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_in_range,
)

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    p = make_paths(ROOT)

    log.info("Loading raw inputs")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    log.info("Rows: orders_raw=%s, users=%s", len(orders_raw), len(users))

    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, name="orders_raw")
    assert_non_empty(users, name="users")

  
    orders = enforce_schema(orders_raw)

  
    rep = missingness_report(orders)

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

   


    rep_qmd_path = reports_dir / "missingness_orders.qmd"
    qmd_text = "\n".join(
        [
            "---",
            'title: "Missingness Report (Orders)"',
            "format: html",
            "---",
            "",
            "## Missingness table",
            "",
            rep.to_markdown(),
            "",
        ]
    )
    rep_qmd_path.write_text(qmd_text, encoding="utf-8")

    log.info("Wrote missingness report (qmd): %s", rep_qmd_path)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders.assign(status_clean=status_clean)
        .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

  
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")

 
    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info("Wrote processed outputs: %s", p.processed)


if __name__ == "__main__":
    main()
