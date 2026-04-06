import duckdb

DB_PATH = "data/duckdb/olist.duckdb"

conn = duckdb.connect(DB_PATH)

print("=" * 50)
print("ESQUEMA DEL DATASET")
print("=" * 50)

tablas = [
    "raw_customers", "raw_orders", "raw_order_items",
    "raw_order_payments", "raw_order_reviews", "raw_products",
    "raw_sellers", "raw_geolocation", "raw_category_translation"
]

for tabla in tablas:
    cols = conn.execute(f"DESCRIBE {tabla}").df()
    total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"\n📋 {tabla} — {total} filas")
    print(cols[["column_name", "column_type"]].to_string(index=False))

print("\n" + "=" * 50)
print("ANÁLISIS DE ÓRDENES")
print("=" * 50)

print("\nEstados de órdenes:")
print(conn.execute("""
    SELECT order_status, COUNT(*) AS total
    FROM raw_orders
    GROUP BY order_status
    ORDER BY total DESC
""").df().to_string(index=False))

print("\nÓrdenes por año:")
print(conn.execute("""
    SELECT
        YEAR(order_purchase_timestamp::TIMESTAMP) AS año,
        COUNT(*) AS total_ordenes
    FROM raw_orders
    GROUP BY año
    ORDER BY año
""").df().to_string(index=False))

print("\n" + "=" * 50)
print("ANÁLISIS DE PAGOS")
print("=" * 50)

print("\nMétodos de pago:")
print(conn.execute("""
    SELECT payment_type, COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
    FROM raw_order_payments
    GROUP BY payment_type
    ORDER BY total DESC
""").df().to_string(index=False))

print("\n" + "=" * 50)
print("ANÁLISIS DE PRODUCTOS")
print("=" * 50)

print("\nTop 10 categorías por cantidad de productos:")
print(conn.execute("""
    SELECT
        t.product_category_name_english AS categoria,
        COUNT(*) AS total_productos
    FROM raw_products p
    LEFT JOIN raw_category_translation t
        ON p.product_category_name = t.product_category_name
    GROUP BY categoria
    ORDER BY total_productos DESC
    LIMIT 10
""").df().to_string(index=False))

conn.close()