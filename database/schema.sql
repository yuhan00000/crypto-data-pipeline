-- schema.sql
-- Create the table used by this project to store daily crypto prices.
--
-- How to apply (example):
--   psql -h localhost -U postgres -d crypto_db -f database/schema.sql

CREATE TABLE IF NOT EXISTS crypto_prices (
  id SERIAL PRIMARY KEY,
  "timestamp" TIMESTAMP NOT NULL,
  bitcoin_price DOUBLE PRECISION NOT NULL,
  ethereum_price DOUBLE PRECISION NOT NULL
);

-- Prevent duplicate rows for the same timestamp.
-- This supports UPSERT logic: INSERT ... ON CONFLICT ("timestamp") DO UPDATE ...
CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_prices_timestamp
  ON crypto_prices ("timestamp");

