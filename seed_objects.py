import psycopg2

DDL = r"""
-- Clean up prior demo objects in dependency order.
DROP MATERIALIZED VIEW IF EXISTS demo_customer_ltv_mv;
DROP VIEW IF EXISTS demo_recent_orders_view;
DROP VIEW IF EXISTS demo_product_sales_view;
DROP VIEW IF EXISTS demo_customer_summary_view;

DROP TABLE IF EXISTS demo_inventory_snapshots;
DROP TABLE IF EXISTS demo_shipments;
DROP TABLE IF EXISTS demo_payments;
DROP TABLE IF EXISTS demo_order_items;
DROP TABLE IF EXISTS demo_orders;
DROP TABLE IF EXISTS demo_products;
DROP TABLE IF EXISTS demo_customers;
DROP TABLE IF EXISTS demo_suppliers;
DROP TABLE IF EXISTS demo_categories;

DROP FUNCTION IF EXISTS demo_calculate_discount(numeric, numeric);
DROP FUNCTION IF EXISTS demo_numeric_avg_accum(numeric[], numeric);
DROP FUNCTION IF EXISTS demo_numeric_avg_final(numeric[]);
DROP AGGREGATE IF EXISTS demo_custom_avg(numeric);

DROP TYPE IF EXISTS demo_address_type CASCADE;
DROP TYPE IF EXISTS demo_customer_tier CASCADE;
DROP TYPE IF EXISTS demo_order_status CASCADE;
DROP SEQUENCE IF EXISTS demo_order_sequence;

CREATE SEQUENCE demo_order_sequence START 1000;

CREATE TYPE demo_order_status AS ENUM (
    'PENDING',
    'PROCESSING',
    'SHIPPED',
    'DELIVERED',
    'CANCELLED'
);

CREATE TYPE demo_customer_tier AS ENUM (
    'TRIAL',
    'STANDARD',
    'PLUS',
    'VIP'
);

CREATE TYPE demo_address_type AS (
    street text,
    city text,
    state text,
    zip text
);

CREATE TABLE demo_categories (
    category_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_category_id integer REFERENCES demo_categories(category_id),
    category_name varchar(80) NOT NULL UNIQUE,
    category_slug varchar(80) NOT NULL UNIQUE,
    display_order smallint NOT NULL,
    is_featured boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE demo_suppliers (
    supplier_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_code varchar(20) NOT NULL UNIQUE,
    company_name varchar(140) NOT NULL,
    contact_name varchar(120) NOT NULL,
    contact_email varchar(160) NOT NULL UNIQUE,
    phone varchar(24),
    country_code char(2) NOT NULL,
    rating numeric(3,2) NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE demo_customers (
    customer_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_uuid uuid NOT NULL UNIQUE,
    email varchar(180) NOT NULL UNIQUE,
    full_name varchar(140) NOT NULL,
    tier demo_customer_tier NOT NULL DEFAULT 'STANDARD',
    birth_date date,
    joined_at timestamptz NOT NULL DEFAULT now(),
    marketing_opt_in boolean NOT NULL DEFAULT false,
    lifetime_credit numeric(12,2) NOT NULL DEFAULT 0,
    tags text[] NOT NULL DEFAULT '{}'::text[],
    profile jsonb NOT NULL DEFAULT '{}'::jsonb,
    home_address demo_address_type
);

CREATE TABLE demo_products (
    product_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku varchar(40) NOT NULL UNIQUE,
    category_id integer NOT NULL REFERENCES demo_categories(category_id),
    supplier_id integer NOT NULL REFERENCES demo_suppliers(supplier_id),
    product_name varchar(180) NOT NULL,
    description text,
    unit_price numeric(10,2) NOT NULL,
    cost_price numeric(10,2) NOT NULL,
    weight_kg numeric(8,3),
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    launched_on date,
    is_active boolean NOT NULL DEFAULT true,
    reorder_threshold integer NOT NULL DEFAULT 10,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE demo_orders (
    order_id bigint PRIMARY KEY DEFAULT nextval('demo_order_sequence'),
    customer_id integer NOT NULL REFERENCES demo_customers(customer_id),
    status demo_order_status NOT NULL,
    ordered_at timestamptz NOT NULL,
    fulfilled_at timestamptz,
    shipping_method varchar(32) NOT NULL,
    shipping_address demo_address_type NOT NULL,
    source_channel varchar(32) NOT NULL,
    subtotal numeric(12,2) NOT NULL,
    tax_amount numeric(12,2) NOT NULL,
    discount_amount numeric(12,2) NOT NULL DEFAULT 0,
    total_amount numeric(12,2) NOT NULL,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE demo_order_items (
    order_item_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES demo_orders(order_id) ON DELETE CASCADE,
    product_id integer NOT NULL REFERENCES demo_products(product_id),
    quantity integer NOT NULL,
    unit_price numeric(10,2) NOT NULL,
    discount_amount numeric(10,2) NOT NULL DEFAULT 0,
    line_total numeric(12,2) NOT NULL
);

CREATE TABLE demo_payments (
    payment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES demo_orders(order_id) ON DELETE CASCADE,
    customer_id integer NOT NULL REFERENCES demo_customers(customer_id),
    payment_method varchar(24) NOT NULL,
    amount numeric(12,2) NOT NULL,
    paid_at timestamptz NOT NULL,
    is_confirmed boolean NOT NULL DEFAULT true,
    gateway_reference varchar(64) NOT NULL UNIQUE,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE demo_shipments (
    shipment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL UNIQUE REFERENCES demo_orders(order_id) ON DELETE CASCADE,
    carrier varchar(48) NOT NULL,
    tracking_number varchar(48) NOT NULL UNIQUE,
    shipped_at timestamptz NOT NULL,
    delivered_at timestamptz,
    shipment_status varchar(24) NOT NULL,
    package_weight_kg numeric(8,3) NOT NULL
);

CREATE TABLE demo_inventory_snapshots (
    snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id integer NOT NULL REFERENCES demo_products(product_id) ON DELETE CASCADE,
    warehouse_code varchar(16) NOT NULL,
    snapshot_at timestamptz NOT NULL,
    quantity_on_hand integer NOT NULL,
    reserved_quantity integer NOT NULL DEFAULT 0,
    bin_location varchar(32),
    UNIQUE (product_id, warehouse_code, snapshot_at)
);

CREATE INDEX idx_demo_orders_customer_id ON demo_orders(customer_id);
CREATE INDEX idx_demo_orders_status ON demo_orders(status);
CREATE INDEX idx_demo_orders_ordered_at ON demo_orders(ordered_at DESC);
CREATE INDEX idx_demo_order_items_order_id ON demo_order_items(order_id);
CREATE INDEX idx_demo_order_items_product_id ON demo_order_items(product_id);
CREATE INDEX idx_demo_products_category_id ON demo_products(category_id);
CREATE INDEX idx_demo_products_supplier_id ON demo_products(supplier_id);
CREATE INDEX idx_demo_inventory_snapshots_product_id ON demo_inventory_snapshots(product_id);
CREATE INDEX idx_demo_customers_joined_at ON demo_customers(joined_at DESC);

INSERT INTO demo_categories (parent_category_id, category_name, category_slug, display_order, is_featured)
VALUES
    (NULL, 'Electronics', 'electronics', 1, true),
    (NULL, 'Home Appliances', 'home-appliances', 2, true),
    (NULL, 'Clothing', 'clothing', 3, false),
    (NULL, 'Books', 'books', 4, false),
    (NULL, 'Sports', 'sports', 5, true),
    (1, 'Audio', 'audio', 6, false),
    (1, 'Computing', 'computing', 7, false),
    (2, 'Kitchen', 'kitchen', 8, false),
    (3, 'Outerwear', 'outerwear', 9, false),
    (5, 'Fitness', 'fitness', 10, false);

INSERT INTO demo_suppliers (
    supplier_code,
    company_name,
    contact_name,
    contact_email,
    phone,
    country_code,
    rating,
    metadata,
    is_active
)
SELECT
    format('SUP-%03s', gs),
    format('Supplier %s Industries', gs),
    format('Contact %s', gs),
    format('supplier%s@example.com', gs),
    format('+1-312-555-%04s', gs),
    CASE gs % 5
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'CA'
        WHEN 2 THEN 'DE'
        WHEN 3 THEN 'JP'
        ELSE 'GB'
    END,
    round((3.20 + ((gs % 16) * 0.11))::numeric, 2),
    jsonb_build_object(
        'lead_time_days', 3 + (gs % 9),
        'supports_drop_ship', (gs % 3 = 0),
        'region', CASE gs % 4 WHEN 0 THEN 'amer' WHEN 1 THEN 'emea' WHEN 2 THEN 'apac' ELSE 'latam' END
    ),
    gs % 11 <> 0
FROM generate_series(1, 36) AS gs;

INSERT INTO demo_customers (
    customer_uuid,
    email,
    full_name,
    tier,
    birth_date,
    joined_at,
    marketing_opt_in,
    lifetime_credit,
    tags,
    profile,
    home_address
)
SELECT
    (
        substr(md5('customer-' || gs), 1, 8) || '-' ||
        substr(md5('customer-' || gs), 9, 4) || '-' ||
        '4' || substr(md5('customer-' || gs), 14, 3) || '-' ||
        'a' || substr(md5('customer-' || gs), 18, 3) || '-' ||
        substr(md5('customer-' || gs), 21, 12)
    )::uuid,
    format('customer%04s@example.com', gs),
    format('Customer %s', gs),
    CASE
        WHEN gs % 17 = 0 THEN 'VIP'::demo_customer_tier
        WHEN gs % 7 = 0 THEN 'PLUS'::demo_customer_tier
        WHEN gs % 5 = 0 THEN 'TRIAL'::demo_customer_tier
        ELSE 'STANDARD'::demo_customer_tier
    END,
    date '1980-01-01' + ((gs * 37) % 11000),
    now() - ((gs % 720) || ' days')::interval,
    gs % 3 = 0,
    round(((gs % 15) * 125.75)::numeric, 2),
    ARRAY[
        CASE WHEN gs % 2 = 0 THEN 'newsletter' ELSE 'organic' END,
        CASE WHEN gs % 5 = 0 THEN 'vip-watch' ELSE 'standard' END
    ],
    jsonb_build_object(
        'preferred_language', CASE gs % 4 WHEN 0 THEN 'en' WHEN 1 THEN 'es' WHEN 2 THEN 'fr' ELSE 'de' END,
        'lifetime_orders_hint', 1 + (gs % 24),
        'support_score', 60 + (gs % 40)
    ),
    ROW(
        format('%s Market Street', 100 + gs),
        CASE gs % 5 WHEN 0 THEN 'Chicago' WHEN 1 THEN 'Austin' WHEN 2 THEN 'Seattle' WHEN 3 THEN 'Denver' ELSE 'Boston' END,
        CASE gs % 5 WHEN 0 THEN 'IL' WHEN 1 THEN 'TX' WHEN 2 THEN 'WA' WHEN 3 THEN 'CO' ELSE 'MA' END,
        format('%05s', 10000 + gs)
    )::demo_address_type
FROM generate_series(1, 650) AS gs;

INSERT INTO demo_products (
    sku,
    category_id,
    supplier_id,
    product_name,
    description,
    unit_price,
    cost_price,
    weight_kg,
    attributes,
    launched_on,
    is_active,
    reorder_threshold
)
SELECT
    format('SKU-%05s', gs),
    1 + (gs % 10),
    1 + (gs % 36),
    format('Demo Product %s', gs),
    format('Product %s seeded for scroll and join testing.', gs),
    round((14.99 + ((gs % 80) * 7.15))::numeric, 2),
    round((8.25 + ((gs % 65) * 4.35))::numeric, 2),
    round((0.15 + ((gs % 50) * 0.23))::numeric, 3),
    jsonb_build_object(
        'color', CASE gs % 6 WHEN 0 THEN 'black' WHEN 1 THEN 'silver' WHEN 2 THEN 'blue' WHEN 3 THEN 'green' WHEN 4 THEN 'red' ELSE 'white' END,
        'size', CASE gs % 4 WHEN 0 THEN 'S' WHEN 1 THEN 'M' WHEN 2 THEN 'L' ELSE 'XL' END,
        'fragile', gs % 9 = 0
    ),
    date '2021-01-01' + ((gs * 11) % 1500),
    gs % 13 <> 0,
    5 + (gs % 40)
FROM generate_series(1, 1800) AS gs;

INSERT INTO demo_orders (
    customer_id,
    status,
    ordered_at,
    fulfilled_at,
    shipping_method,
    shipping_address,
    source_channel,
    subtotal,
    tax_amount,
    discount_amount,
    total_amount,
    notes
)
SELECT
    1 + ((gs * 17) % 650),
    CASE
        WHEN gs % 19 = 0 THEN 'CANCELLED'::demo_order_status
        WHEN gs % 7 = 0 THEN 'DELIVERED'::demo_order_status
        WHEN gs % 5 = 0 THEN 'SHIPPED'::demo_order_status
        WHEN gs % 3 = 0 THEN 'PROCESSING'::demo_order_status
        ELSE 'PENDING'::demo_order_status
    END,
    now() - ((gs % 540) || ' hours')::interval,
    CASE
        WHEN gs % 19 = 0 THEN NULL
        ELSE now() - (((gs % 540) - (1 + (gs % 48))) || ' hours')::interval
    END,
    CASE gs % 4 WHEN 0 THEN 'standard' WHEN 1 THEN 'express' WHEN 2 THEN 'pickup' ELSE 'two_day' END,
    ROW(
        format('%s Warehouse Road', 500 + gs),
        CASE gs % 5 WHEN 0 THEN 'Chicago' WHEN 1 THEN 'Austin' WHEN 2 THEN 'Seattle' WHEN 3 THEN 'Denver' ELSE 'Boston' END,
        CASE gs % 5 WHEN 0 THEN 'IL' WHEN 1 THEN 'TX' WHEN 2 THEN 'WA' WHEN 3 THEN 'CO' ELSE 'MA' END,
        format('%05s', 20000 + (gs % 5000))
    )::demo_address_type,
    CASE gs % 4 WHEN 0 THEN 'web' WHEN 1 THEN 'mobile' WHEN 2 THEN 'partner' ELSE 'sales' END,
    round((50 + ((gs % 35) * 18.4))::numeric, 2),
    round((4 + ((gs % 9) * 2.15))::numeric, 2),
    round(((gs % 6) * 3.75)::numeric, 2),
    round((54 + ((gs % 35) * 18.4) + (4 + ((gs % 9) * 2.15)) - ((gs % 6) * 3.75))::numeric, 2),
    CASE WHEN gs % 14 = 0 THEN 'Priority customer follow-up' ELSE NULL END
FROM generate_series(1, 5000) AS gs;

INSERT INTO demo_order_items (
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    line_total
)
SELECT
    o.order_id,
    1 + ((o.order_id::int * 13 + item_idx.n) % 1800),
    1 + ((o.order_id::int + item_idx.n) % 4),
    round((12.50 + (((o.order_id::int + item_idx.n) % 90) * 6.10))::numeric, 2),
    round((((o.order_id::int + item_idx.n) % 3) * 1.75)::numeric, 2),
    round(((1 + ((o.order_id::int + item_idx.n) % 4)) * (12.50 + (((o.order_id::int + item_idx.n) % 90) * 6.10)) - (((o.order_id::int + item_idx.n) % 3) * 1.75))::numeric, 2)
FROM demo_orders o
CROSS JOIN LATERAL generate_series(1, 1 + ((o.order_id::int % 4))) AS item_idx(n);

INSERT INTO demo_payments (
    order_id,
    customer_id,
    payment_method,
    amount,
    paid_at,
    is_confirmed,
    gateway_reference,
    metadata
)
SELECT
    o.order_id,
    o.customer_id,
    CASE o.order_id % 4 WHEN 0 THEN 'card' WHEN 1 THEN 'wallet' WHEN 2 THEN 'wire' ELSE 'gift_card' END,
    o.total_amount,
    o.ordered_at + ((o.order_id % 6) || ' hours')::interval,
    o.status <> 'CANCELLED',
    format('pay_%s_%s', o.order_id, o.customer_id),
    jsonb_build_object(
        'captured', o.status <> 'CANCELLED',
        'risk_score', 5 + (o.order_id % 70),
        'processor', CASE o.order_id % 3 WHEN 0 THEN 'stripe' WHEN 1 THEN 'adyen' ELSE 'braintree' END
    )
FROM demo_orders o;

INSERT INTO demo_shipments (
    order_id,
    carrier,
    tracking_number,
    shipped_at,
    delivered_at,
    shipment_status,
    package_weight_kg
)
SELECT
    o.order_id,
    CASE o.order_id % 4 WHEN 0 THEN 'UPS' WHEN 1 THEN 'FedEx' WHEN 2 THEN 'USPS' ELSE 'DHL' END,
    format('TRK%010s', o.order_id),
    o.ordered_at + ((1 + (o.order_id % 48)) || ' hours')::interval,
    CASE
        WHEN o.status = 'DELIVERED' THEN o.ordered_at + ((48 + (o.order_id % 96)) || ' hours')::interval
        ELSE NULL
    END,
    CASE
        WHEN o.status = 'DELIVERED' THEN 'delivered'
        WHEN o.status = 'SHIPPED' THEN 'in_transit'
        WHEN o.status = 'PROCESSING' THEN 'label_created'
        ELSE 'pending'
    END,
    round((0.45 + ((o.order_id % 18) * 0.38))::numeric, 3)
FROM demo_orders o
WHERE o.status IN ('PROCESSING', 'SHIPPED', 'DELIVERED');

INSERT INTO demo_inventory_snapshots (
    product_id,
    warehouse_code,
    snapshot_at,
    quantity_on_hand,
    reserved_quantity,
    bin_location
)
SELECT
    p.product_id,
    wh.code,
    now() - ((snap.n * 6) || ' hours')::interval,
    15 + ((p.product_id + snap.n * 7) % 180),
    (p.product_id + snap.n) % 12,
    format('%s-%02s-%02s', wh.code, 1 + (p.product_id % 12), 1 + (snap.n % 30))
FROM demo_products p
CROSS JOIN (VALUES ('CHI'), ('AUS'), ('SEA')) AS wh(code)
CROSS JOIN LATERAL generate_series(1, 2) AS snap(n)
WHERE p.product_id <= 600;

CREATE OR REPLACE VIEW demo_recent_orders_view AS
SELECT
    o.order_id,
    o.ordered_at,
    o.status,
    o.total_amount,
    o.source_channel,
    c.customer_id,
    c.full_name,
    c.tier,
    c.email
FROM demo_orders o
JOIN demo_customers c ON c.customer_id = o.customer_id
WHERE o.ordered_at >= now() - interval '30 days';

CREATE OR REPLACE VIEW demo_product_sales_view AS
SELECT
    p.product_id,
    p.sku,
    p.product_name,
    c.category_name,
    s.company_name AS supplier_name,
    count(DISTINCT oi.order_id) AS order_count,
    sum(oi.quantity) AS units_sold,
    round(sum(oi.line_total)::numeric, 2) AS gross_revenue,
    round(avg(oi.unit_price)::numeric, 2) AS avg_sell_price
FROM demo_products p
JOIN demo_categories c ON c.category_id = p.category_id
JOIN demo_suppliers s ON s.supplier_id = p.supplier_id
LEFT JOIN demo_order_items oi ON oi.product_id = p.product_id
GROUP BY p.product_id, p.sku, p.product_name, c.category_name, s.company_name;

CREATE OR REPLACE VIEW demo_customer_summary_view AS
SELECT
    c.customer_id,
    c.customer_uuid,
    c.full_name,
    c.tier,
    c.joined_at,
    count(DISTINCT o.order_id) AS total_orders,
    round(coalesce(sum(o.total_amount), 0)::numeric, 2) AS gross_spend,
    max(o.ordered_at) AS last_ordered_at
FROM demo_customers c
LEFT JOIN demo_orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_uuid, c.full_name, c.tier, c.joined_at;

CREATE MATERIALIZED VIEW demo_customer_ltv_mv AS
SELECT
    c.customer_id,
    c.full_name,
    c.tier,
    count(DISTINCT o.order_id) AS total_orders,
    round(coalesce(sum(o.total_amount), 0)::numeric, 2) AS lifetime_value,
    round(coalesce(avg(o.total_amount), 0)::numeric, 2) AS average_order_value
FROM demo_customers c
LEFT JOIN demo_orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, c.tier;

CREATE OR REPLACE FUNCTION demo_calculate_discount(total_amount numeric, discount_percent numeric)
RETURNS numeric AS $func$
BEGIN
    RETURN round(total_amount - (total_amount * (discount_percent / 100.0)), 2);
END;
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION demo_numeric_avg_accum(state numeric[], incoming numeric)
RETURNS numeric[] AS $func$
BEGIN
    IF incoming IS NULL THEN
        RETURN state;
    END IF;
    RETURN ARRAY[state[1] + incoming, state[2] + 1];
END;
$func$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION demo_numeric_avg_final(state numeric[])
RETURNS numeric AS $func$
BEGIN
    IF state[2] = 0 THEN
        RETURN 0;
    END IF;
    RETURN round(state[1] / state[2], 2);
END;
$func$ LANGUAGE plpgsql;

CREATE AGGREGATE demo_custom_avg(numeric) (
    sfunc = demo_numeric_avg_accum,
    stype = numeric[],
    finalfunc = demo_numeric_avg_final,
    initcond = '{0,0}'
);
"""

COUNTS_SQL = """
SELECT 'demo_categories' AS object_name, count(*) AS row_count FROM demo_categories
UNION ALL
SELECT 'demo_suppliers', count(*) FROM demo_suppliers
UNION ALL
SELECT 'demo_customers', count(*) FROM demo_customers
UNION ALL
SELECT 'demo_products', count(*) FROM demo_products
UNION ALL
SELECT 'demo_orders', count(*) FROM demo_orders
UNION ALL
SELECT 'demo_order_items', count(*) FROM demo_order_items
UNION ALL
SELECT 'demo_payments', count(*) FROM demo_payments
UNION ALL
SELECT 'demo_shipments', count(*) FROM demo_shipments
UNION ALL
SELECT 'demo_inventory_snapshots', count(*) FROM demo_inventory_snapshots
ORDER BY object_name;
"""


def connect():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="retail_db",
        user="admin",
        password="password123",
    )


def main():
    conn = None
    try:
        try:
            conn = connect()
        except psycopg2.OperationalError:
            conn = psycopg2.connect("postgresql://admin:password123@localhost:5432/retail_db")

        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(COUNTS_SQL)
            print("Seeded demo objects in public schema:")
            for object_name, row_count in cur.fetchall():
                print(f"  {object_name:<24} {row_count}")
            print("Views: demo_recent_orders_view, demo_product_sales_view, demo_customer_summary_view")
            print("Materialized view: demo_customer_ltv_mv")
    except Exception as exc:
        print(f"Seeding failed: {exc}")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
