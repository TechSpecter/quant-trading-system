-- ============================================
-- Enable TimescaleDB Extension (Best Practice)
-- ============================================
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================
-- Market Data Table
-- ============================================
CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY(symbol, timeframe, timestamp)
);

-- ============================================
-- Convert to Timescale Hypertable
-- ============================================
SELECT create_hypertable(
    'candles',
    'timestamp',
    if_not_exists => TRUE
);

-- ============================================
-- Indexes for Fast Queries
-- ============================================
-- Main query index used for backtests & MTF alignment
CREATE INDEX IF NOT EXISTS idx_candles_symbol_timeframe_timestamp
ON candles(symbol, timeframe, timestamp DESC);

-- Faster symbol scans
CREATE INDEX IF NOT EXISTS idx_candles_symbol
ON candles(symbol);

-- Faster timeframe queries
CREATE INDEX IF NOT EXISTS idx_candles_timeframe
ON candles(timeframe);
