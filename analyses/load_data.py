import duckdb
import os

DB_PATH = "data/duckdb/olist.duckdb"
RAW_PATH = "data/raw"

# Mapeo de archivos a nombres de tablas
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
    for archivo, tabla in TABLAS.items():
        ruta = os.path.join(RAW_PATH, archivo)
        conn.execute(f"DROP TABLE IF EXISTS {tabla}")
        conn.execute(f"""
            CREATE TABLE {tabla} AS
            SELECT * FROM read_csv_auto('{ruta}')
        """)
        total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        print(f"{tabla}: {total} filas")


if __name__ == "__main__":
    conn = duckdb.connect(DB_PATH)
    cargar_tablas(conn)
    conn.close()
    print("\nCarga completada.")