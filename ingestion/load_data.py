import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.logger import get_logger
from utils.quality import check_nulls, check_duplicates, check_range, check_row_count

logger = get_logger("ingestion")

DB_PATH = "data/duckdb/olist.duckdb"
RAW_PATH = "data/raw"

TABLAS = {
    "olist_customers_dataset.csv":            "raw_customers",
    "olist_geolocation_dataset.csv":          "raw_geolocation",
    "olist_order_items_dataset.csv":          "raw_order_items",
    "olist_order_payments_dataset.csv":       "raw_order_payments",
    "olist_order_reviews_dataset.csv":        "raw_order_reviews",
    "olist_orders_dataset.csv":               "raw_orders",
    "olist_products_dataset.csv":             "raw_products",
    "olist_sellers_dataset.csv":              "raw_sellers",
    "product_category_name_translation.csv":  "raw_category_translation",
}


def cargar_tablas(conn: duckdb.DuckDBPyConnection):
    logger.info("Iniciando carga de tablas raw")
    for archivo, tabla in TABLAS.items():
        try:
            ruta = os.path.join(RAW_PATH, archivo)
            conn.execute(f"DROP TABLE IF EXISTS {tabla}")
            conn.execute(f"""
                CREATE TABLE {tabla} AS
                SELECT * FROM read_csv_auto('{ruta}')
            """)
            total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
            logger.info(f"{tabla} cargada — {total} filas")
        except Exception as e:
            logger.error(f"Error cargando {tabla}: {e}")
            raise


def validar_datos(conn: duckdb.DuckDBPyConnection):
    logger.info("Iniciando validaciones de calidad")

    # Customers
    df = conn.execute("SELECT * FROM raw_customers").df()
    check_row_count(df, "raw_customers", 90000)
    check_nulls(df, ["customer_id", "customer_unique_id", "customer_state"], "raw_customers")
    check_duplicates(df, "customer_id", "raw_customers")

    # Orders
    df = conn.execute("SELECT * FROM raw_orders").df()
    check_row_count(df, "raw_orders", 90000)
    check_nulls(df, ["order_id", "customer_id", "order_status"], "raw_orders")
    check_duplicates(df, "order_id", "raw_orders")

    # Order items
    df = conn.execute("SELECT * FROM raw_order_items").df()
    check_row_count(df, "raw_order_items", 100000)
    check_nulls(df, ["order_id", "product_id", "seller_id"], "raw_order_items")
    check_range(df, "price", "raw_order_items", min_val=0)
    check_range(df, "freight_value", "raw_order_items", min_val=0)

    # Payments
    df = conn.execute("SELECT * FROM raw_order_payments").df()
    check_nulls(df, ["order_id", "payment_type", "payment_value"], "raw_order_payments")
    check_range(df, "payment_value", "raw_order_payments", min_val=0)

    logger.info("Validaciones completadas")


if __name__ == "__main__":
    try:
        conn = duckdb.connect(DB_PATH)
        cargar_tablas(conn)
        validar_datos(conn)
        conn.close()
        logger.info("Ingesta finalizada exitosamente")
    except Exception as e:
        logger.error(f"Pipeline de ingesta falló: {e}")
        raise