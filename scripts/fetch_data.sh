#!/usr/bin/env bash
# fetch_data.sh — download the gitignored data files this project needs.
#
# Usage (from anywhere):   bash scripts/fetch_data.sh [--force]
#
# 1. Downloads the public Olist dataset zip from Kaggle (no login or API key needed).
# 2. Extracts the 4 CSVs the Airflow DAG reads into airflow/include/data/.
# 3. Verifies their SHA-256 checksums against the copies the project was built with.
# 4. Rebuilds great_expectations/data/orders_dq.csv with scripts/build_orders_dq.py
#    (needs pandas; set PYTHON=/path/to/python to pick an interpreter/venv).
#
# Requires: curl, unzip, shasum or sha256sum (all preinstalled on macOS).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/airflow/include/data"
DQ_OUT="$ROOT/great_expectations/data/orders_dq.csv"
URL="https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
PYTHON="${PYTHON:-python3}"
FORCE="${1:-}"

# file name -> expected sha256
CHECKSUMS="983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2  olist_customers_dataset.csv
0bc4d068c4fe38cbb01bd90e8746e3c613fe7b4baef75fab7b0e329701c3e279  olist_order_items_dataset.csv
8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf  olist_orders_dataset.csv
3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc  olist_products_dataset.csv"

sha256() {
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else sha256sum "$1" | cut -d' ' -f1; fi
}

all_present() {
  while read -r sum name; do
    [ -f "$DATA_DIR/$name" ] && [ "$(sha256 "$DATA_DIR/$name")" = "$sum" ] || return 1
  done <<< "$CHECKSUMS"
}

mkdir -p "$DATA_DIR" "$(dirname "$DQ_OUT")"

if [ "$FORCE" != "--force" ] && all_present; then
  echo "[fetch] Olist CSVs already present and verified in airflow/include/data/"
else
  TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
  echo "[fetch] Downloading Olist dataset (~45 MB) from Kaggle..."
  curl -fsSL -o "$TMP/olist.zip" "$URL"
  unzip -q -j -o "$TMP/olist.zip" \
    olist_orders_dataset.csv olist_customers_dataset.csv \
    olist_products_dataset.csv olist_order_items_dataset.csv -d "$DATA_DIR"
  while read -r sum name; do
    actual="$(sha256 "$DATA_DIR/$name")"
    if [ "$actual" = "$sum" ]; then echo "[fetch] OK  $name"
    else echo "[fetch] WARNING checksum mismatch for $name (dataset may have been updated on Kaggle)"; fi
  done <<< "$CHECKSUMS"
fi

echo "[fetch] Building great_expectations/data/orders_dq.csv ..."
if "$PYTHON" -c "import pandas" 2>/dev/null; then
  "$PYTHON" "$ROOT/scripts/build_orders_dq.py" "$DATA_DIR" "$DQ_OUT"
else
  echo "[fetch] pandas not found for '$PYTHON'. Install it (pip install pandas) and rerun, or run:"
  echo "        PYTHON=great_expectations/venv/bin/python bash scripts/fetch_data.sh"
  exit 1
fi
echo "[fetch] Done."
