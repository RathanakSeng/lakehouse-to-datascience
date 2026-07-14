-- sql/seed_products.sql
-- Run after schema.sql. Seeds products only; orders stays empty so the
-- CDC snapshot phase captures it as-is.

INSERT INTO public.products (name, category, unit_price, stock_quantity) VALUES
    ('Aluminum wok, 30cm',             'Cookware',  8.50, 40),
    ('Stone mortar and pestle',        'Cookware', 12.00, 25),
    ('Clay charcoal stove',            'Cookware', 15.00, 15),
    ('Non-stick frying pan, imported', 'Cookware', 18.00, 20),
    ('Bamboo steamer basket',          'Cookware',  6.00, 30),
    ('Rice cooker, 1.8L',              'Appliance',22.00, 18);

INSERT INTO public.orders (customer_name, product_id, quantity, order_status) VALUES
    (
        'Mr. Bunthoeun',
        (SELECT product_id FROM public.products WHERE name = 'Aluminum wok, 30cm'),
        2,
        'placed'
    );

