import os
import sqlite3
from datetime import datetime, timedelta

DB_PATH = "data/ecommerce.db"

def get_db_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def run_aggregate_query(conn, start_date, end_date):
    query = """
        SELECT
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)), 0) AS total_revenue
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        WHERE DATE(o.order_date) BETWEEN ? AND ?
          AND o.status != 'CANCELLED';
    """
    cursor = conn.cursor()
    cursor.execute(query, (start_date, end_date))
    return cursor.fetchone()

def get_top_products(conn, start_date, end_date):
    query = """
        SELECT
            p.product_name,
            SUM(oi.quantity) AS total_quantity,
            SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)) AS product_revenue
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE DATE(o.order_date) BETWEEN ? AND ?
          AND o.status != 'CANCELLED'
        GROUP BY p.product_id
        ORDER BY product_revenue DESC
        LIMIT 3;
    """
    cursor = conn.cursor()
    cursor.execute(query, (start_date, end_date))
    return cursor.fetchall()

def calculate_previous_period(start_str, end_str):
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    delta = (end_dt - start_dt).days + 1
    prev_end_dt = start_dt - timedelta(days=1)
    prev_start_dt = prev_end_dt - timedelta(days=delta - 1)
    return prev_start_dt.strftime("%Y-%m-%d"), prev_end_dt.strftime("%Y-%m-%d")

def generate_report(report_type, start_str, end_str):
    try:
        conn = get_db_connection()
    except Exception as e:
        return

    prev_start_str, prev_end_str = calculate_previous_period(start_str, end_str)

    curr_metrics = run_aggregate_query(conn, start_str, end_str)
    prev_metrics = run_aggregate_query(conn, prev_start_str, prev_end_str)
    top_products = get_top_products(conn, start_str, end_str)

    orders_pct = 0.0
    if prev_metrics['total_orders'] > 0:
        orders_pct = ((curr_metrics['total_orders'] - prev_metrics['total_orders']) / prev_metrics['total_orders']) * 100.0

    revenue_pct = 0.0
    if prev_metrics['total_revenue'] > 0:
        revenue_pct = ((curr_metrics['total_revenue'] - prev_metrics['total_revenue']) / prev_metrics['total_revenue']) * 100.0

    custs_pct = 0.0
    if prev_metrics['unique_customers'] > 0:
        custs_pct = ((curr_metrics['unique_customers'] - prev_metrics['unique_customers']) / prev_metrics['unique_customers']) * 100.0

    print(f"Total Orders:     {curr_metrics['total_orders']} (Prev: {prev_metrics['total_orders']}, Change: {orders_pct:+.1f}%)")
    print(f"Total Revenue:    ${curr_metrics['total_revenue']:,.2f} (Prev: ${prev_metrics['total_revenue']:,.2f}, Change: {revenue_pct:+.1f}%)")
    print(f"Unique Customers: {curr_metrics['unique_customers']} (Prev: {prev_metrics['unique_customers']}, Change: {custs_pct:+.1f}%)")
    
    if top_products:
        for idx, row in enumerate(top_products, 1):
            print(f"  {idx}. {row['product_name']} - Qty: {row['total_quantity']}, Revenue: ${row['product_revenue']:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    report_type = input().strip().lower()
    start_date = input().strip()
    end_date = input().strip()
    generate_report(report_type, start_date, end_date)
