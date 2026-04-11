-- Habilitar la extensión necesaria para logical replication
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tabla de órdenes (fuente CDC)
CREATE TABLE orders (
    order_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id INT NOT NULL,
    product     VARCHAR(100) NOT NULL,
    quantity    INT NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Tabla de clientes
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(100) UNIQUE NOT NULL,
    country     VARCHAR(50),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Datos semilla
INSERT INTO customers (customer_id, name, email, country) VALUES
(1, 'Alice Smith',   'alice@example.com',   'US'),
(2, 'Bob Johnson',   'bob@example.com',     'CA'),
(3, 'Carlos Lopez',  'carlos@example.com',  'MX'),
(4, 'Diana Prince',  'diana@example.com',   'UK');

INSERT INTO orders (customer_id, product, quantity, amount, status) VALUES
(1, 'Laptop',    1, 1200.00, 'completed'),
(2, 'Monitor',   2,  600.00, 'pending'),
(3, 'Keyboard',  3,   90.00, 'completed'),
(4, 'Headset',   1,  150.00, 'shipped'),
(1, 'Mouse',     2,   50.00, 'pending');

-- Publicación para DMS (CDC)
CREATE PUBLICATION dms_publication FOR TABLE orders, customers;
