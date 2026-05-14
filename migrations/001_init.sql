-- ============================================================
-- Migration 001 — initial schema
-- Runs automatically on first docker-compose up via
-- /docker-entrypoint-initdb.d mount
-- ============================================================

CREATE DATABASE IF NOT EXISTS urlshortener;
USE urlshortener;

-- ─── urls ────────────────────────────────────────────────────
-- Stores every shortened URL.
-- short_code: 7-char Base62 string (e.g. "aB3xZ9q")
-- expires_at: NULL means no expiry
CREATE TABLE IF NOT EXISTS urls (
  id          BIGINT        NOT NULL AUTO_INCREMENT,
  short_code  VARCHAR(10)   NOT NULL,
  long_url    TEXT          NOT NULL,
  user_id     BIGINT        NULL,
  created_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at  TIMESTAMP     NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_short_code (short_code),
  KEY idx_user_id (user_id),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── click_events ────────────────────────────────────────────
-- Persistent log written by Kafka workers.
-- This is the source of truth for analytics queries.
-- referrer: domain extracted from Referer header (e.g. "google.com")
CREATE TABLE IF NOT EXISTS click_events (
  id          BIGINT        NOT NULL AUTO_INCREMENT,
  short_code  VARCHAR(10)   NOT NULL,
  referrer    VARCHAR(500)  NULL,
  clicked_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_short_code (short_code),
  KEY idx_clicked_at (clicked_at),
  KEY idx_code_time  (short_code, clicked_at)   -- composite for time-range queries
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─── Seed data for local development ─────────────────────────
INSERT INTO urls (short_code, long_url, created_at)
VALUES
  ('abc1234', 'https://github.com', NOW()),
  ('xyz9876', 'https://go.dev/doc', NOW());