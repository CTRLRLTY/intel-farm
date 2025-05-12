CREATE TABLE IF NOT EXISTS services (
    -- id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    target INET REFERENCES targets(ip_addr) ON DELETE CASCADE,
    name TEXT,
    status TEXT,
    port SMALLINT,
    PRIMARY KEY (target, port)
);