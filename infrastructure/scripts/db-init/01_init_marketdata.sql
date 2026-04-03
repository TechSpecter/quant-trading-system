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

-- ============================================
-- Add Source Column (Multi-Provider Support)
-- ============================================
ALTER TABLE candles ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'fyers';

-- ============================================
-- FII / DII Data Table
-- ============================================
CREATE TABLE IF NOT EXISTS fii_dii (
    date DATE PRIMARY KEY,
    fii_buy DOUBLE PRECISION,
    fii_sell DOUBLE PRECISION,
    fii_net DOUBLE PRECISION,
    dii_buy DOUBLE PRECISION,
    dii_sell DOUBLE PRECISION,
    dii_net DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fii_dii_date
ON fii_dii(date DESC);

-- ============================================
-- News Table (Sentiment Layer)
-- ============================================
CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,          -- rss / twitter
    title TEXT,
    content TEXT,
    symbol TEXT,                  -- optional tagging (e.g. NSE:RELIANCE-EQ)
    sentiment_score DOUBLE PRECISION,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_published_at
ON news(published_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_symbol
ON news(symbol);

CREATE INDEX IF NOT EXISTS idx_news_source
ON news(source);

CREATE INDEX IF NOT EXISTS idx_candles_source
ON candles(source);