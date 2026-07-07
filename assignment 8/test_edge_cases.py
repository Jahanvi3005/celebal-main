import sqlite3
import unittest
from datetime import datetime, timedelta

class TestEcommerceEdgeCases(unittest.TestCase):
    
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        
        self.cursor.execute("""
        CREATE TABLE customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            registration_date TEXT,
            customer_type TEXT
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            subcategory TEXT,
            cost_price REAL
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_date TEXT,
            status TEXT,
            region_code TEXT
        );
        """)
        
        self.cursor.execute("""
        CREATE TABLE order_items (
            item_id TEXT PRIMARY KEY,
            order_id TEXT,
            product_id TEXT,
            quantity INTEGER,
            unit_price REAL,
            discount_percent REAL,
            FOREIGN KEY(order_id) REFERENCES orders(order_id),
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        );
        """)
        self.conn.commit()
        
        self.cursor.execute("INSERT INTO customers VALUES ('CUST001', 'Test Customer', 'test@example.com', '2025-01-01', 'REGULAR')")
        self.cursor.execute("INSERT INTO products VALUES ('PROD001', 'Test Product', 'Electronics', 'Phones', 100.0)")
        self.cursor.execute("INSERT INTO orders VALUES ('ORD001', 'CUST001', '2025-06-01 10:00:00', 'PLACED', 'US-E')")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_referential_integrity_violation(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.cursor.execute("""
                INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, discount_percent)
                VALUES ('ITEM999', 'ORD999', 'PROD001', 2, 120.0, 10.0)
            """)
            self.conn.commit()

    def test_invalid_discount_percent(self):
        discount = 110.0
        
        def validate_discount(d):
            if d > 100 or d < 0:
                raise ValueError("Discount percent must be between 0 and 100")
            return d
            
        with self.assertRaises(ValueError):
            validate_discount(discount)

    def test_zero_quantity(self):
        quantity = 0
        
        def validate_quantity(q):
            if q == 0:
                raise ValueError("Quantity cannot be 0")
            return q
            
        with self.assertRaises(ValueError):
            validate_quantity(quantity)

    def test_future_order_date(self):
        future_date = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        
        def validate_order_date(date_str):
            order_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            if order_dt > datetime.now():
                raise ValueError("Order date cannot be in the future")
            return order_dt
            
        with self.assertRaises(ValueError):
            validate_order_date(future_date)

if __name__ == "__main__":
    unittest.main()
