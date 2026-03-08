-- Consolidated latest schema.
-- This file represents the final schema after applying:
--   - db/schema.sql
--   - db/migrations/001_init.sql
--   - db/migrations/002_add_activity_location_text.sql
--   - db/migrations/003_add_venue_indexes.sql
--   - db/migrations/004_add_activity_query_indexes.sql

CREATE TABLE IF NOT EXISTS sources (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  base_url VARCHAR(1024) NOT NULL,
  adapter_type VARCHAR(100) NOT NULL,
  crawl_frequency VARCHAR(100) NOT NULL DEFAULT 'daily',
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS venues (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(512) NULL,
  city VARCHAR(255) NULL,
  state VARCHAR(50) NULL,
  zip VARCHAR(20) NULL,
  lat DECIMAL(10,7) NULL,
  lng DECIMAL(10,7) NULL,
  website VARCHAR(1024) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_venues_name_city_state (name, city, state),
  INDEX idx_venues_city (city),
  INDEX idx_venues_state (state)
);

CREATE TABLE IF NOT EXISTS activities (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_id BIGINT NOT NULL,
  source_url VARCHAR(1024) NOT NULL,
  external_id VARCHAR(255) NULL,

  title VARCHAR(512) NOT NULL,
  description TEXT NULL,
  activity_type VARCHAR(100) NULL,
  age_min INT NULL,
  age_max INT NULL,

  is_free BOOLEAN NOT NULL DEFAULT TRUE,
  free_verification_status ENUM('confirmed','inferred','uncertain') NOT NULL DEFAULT 'inferred',

  drop_in BOOLEAN NULL,
  registration_required BOOLEAN NULL,

  start_at DATETIME NOT NULL,
  end_at DATETIME NULL,
  timezone VARCHAR(64) NOT NULL DEFAULT 'America/Los_Angeles',
  recurrence_text VARCHAR(512) NULL,
  location_text VARCHAR(512) NULL,

  venue_id BIGINT NULL,

  extraction_method ENUM('hardcoded','llm') NOT NULL DEFAULT 'hardcoded',
  extractor_version VARCHAR(64) NULL,
  llm_provider VARCHAR(64) NULL,
  llm_model VARCHAR(128) NULL,
  llm_confidence DECIMAL(5,4) NULL,

  status ENUM('active','cancelled','expired','needs_review') NOT NULL DEFAULT 'active',
  confidence_score DECIMAL(5,4) NOT NULL DEFAULT 0.8000,

  first_seen_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,

  CONSTRAINT chk_activities_is_free_true CHECK (is_free = TRUE),
  CONSTRAINT fk_activities_source FOREIGN KEY (source_id) REFERENCES sources(id),
  CONSTRAINT fk_activities_venue FOREIGN KEY (venue_id) REFERENCES venues(id),

  INDEX idx_activities_status_start_at (status, start_at),
  INDEX idx_activities_status_venue (status, venue_id),
  INDEX idx_activities_source_lookup (source_id, source_url(191), title(191), start_at),
  INDEX idx_activities_source_url (source_url(255))
);

CREATE TABLE IF NOT EXISTS activity_tags (
  activity_id BIGINT NOT NULL,
  tag VARCHAR(100) NOT NULL,
  PRIMARY KEY (activity_id, tag),
  CONSTRAINT fk_activity_tags_activity FOREIGN KEY (activity_id) REFERENCES activities(id)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  source_id BIGINT NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME NULL,
  status ENUM('running','success','failed') NOT NULL,
  items_found INT NOT NULL DEFAULT 0,
  items_saved INT NOT NULL DEFAULT 0,
  errors TEXT NULL,
  CONSTRAINT fk_ingestion_runs_source FOREIGN KEY (source_id) REFERENCES sources(id),
  INDEX idx_ingestion_runs_source_started (source_id, started_at)
);
