-- sql/schema.sql
-- Pisey's shop operational database (pisey_shop).

CREATE TABLE public.products (
    product_id     SERIAL PRIMARY KEY,
    name           TEXT NOT NULL,
    category       TEXT NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
    order_id      SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    product_id    INTEGER NOT NULL REFERENCES public.products(product_id),
    quantity      INTEGER NOT NULL,
    order_status  TEXT NOT NULL DEFAULT 'placed',
    ordered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
