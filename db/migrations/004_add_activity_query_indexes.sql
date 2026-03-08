-- Optimize index coverage for frontend filters and crawler upserts.

SET @idx_venues_name_city_state_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'venues'
    AND INDEX_NAME = 'idx_venues_name_city_state'
);
SET @ddl_add_venues_name_city_state := IF(
  @idx_venues_name_city_state_exists = 0,
  'ALTER TABLE venues ADD INDEX idx_venues_name_city_state (name, city, state)',
  'SELECT "idx_venues_name_city_state already exists"'
);
PREPARE stmt_add_venues_name_city_state FROM @ddl_add_venues_name_city_state;
EXECUTE stmt_add_venues_name_city_state;
DEALLOCATE PREPARE stmt_add_venues_name_city_state;

SET @idx_venues_name_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'venues'
    AND INDEX_NAME = 'idx_venues_name'
);
SET @ddl_drop_venues_name := IF(
  @idx_venues_name_exists > 0,
  'ALTER TABLE venues DROP INDEX idx_venues_name',
  'SELECT "idx_venues_name not present"'
);
PREPARE stmt_drop_venues_name FROM @ddl_drop_venues_name;
EXECUTE stmt_drop_venues_name;
DEALLOCATE PREPARE stmt_drop_venues_name;

SET @idx_activities_status_start_at_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_status_start_at'
);
SET @ddl_add_activities_status_start_at := IF(
  @idx_activities_status_start_at_exists = 0,
  'ALTER TABLE activities ADD INDEX idx_activities_status_start_at (status, start_at)',
  'SELECT "idx_activities_status_start_at already exists"'
);
PREPARE stmt_add_activities_status_start_at FROM @ddl_add_activities_status_start_at;
EXECUTE stmt_add_activities_status_start_at;
DEALLOCATE PREPARE stmt_add_activities_status_start_at;

SET @idx_activities_status_venue_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_status_venue'
);
SET @ddl_add_activities_status_venue := IF(
  @idx_activities_status_venue_exists = 0,
  'ALTER TABLE activities ADD INDEX idx_activities_status_venue (status, venue_id)',
  'SELECT "idx_activities_status_venue already exists"'
);
PREPARE stmt_add_activities_status_venue FROM @ddl_add_activities_status_venue;
EXECUTE stmt_add_activities_status_venue;
DEALLOCATE PREPARE stmt_add_activities_status_venue;

SET @idx_activities_source_lookup_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_source_lookup'
);
SET @ddl_add_activities_source_lookup := IF(
  @idx_activities_source_lookup_exists = 0,
  'ALTER TABLE activities ADD INDEX idx_activities_source_lookup (source_id, source_url(191), title(191), start_at)',
  'SELECT "idx_activities_source_lookup already exists"'
);
PREPARE stmt_add_activities_source_lookup FROM @ddl_add_activities_source_lookup;
EXECUTE stmt_add_activities_source_lookup;
DEALLOCATE PREPARE stmt_add_activities_source_lookup;

SET @idx_activities_start_at_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_start_at'
);
SET @ddl_drop_activities_start_at := IF(
  @idx_activities_start_at_exists > 0,
  'ALTER TABLE activities DROP INDEX idx_activities_start_at',
  'SELECT "idx_activities_start_at not present"'
);
PREPARE stmt_drop_activities_start_at FROM @ddl_drop_activities_start_at;
EXECUTE stmt_drop_activities_start_at;
DEALLOCATE PREPARE stmt_drop_activities_start_at;

SET @idx_activities_status_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_status'
);
SET @ddl_drop_activities_status := IF(
  @idx_activities_status_exists > 0,
  'ALTER TABLE activities DROP INDEX idx_activities_status',
  'SELECT "idx_activities_status not present"'
);
PREPARE stmt_drop_activities_status FROM @ddl_drop_activities_status;
EXECUTE stmt_drop_activities_status;
DEALLOCATE PREPARE stmt_drop_activities_status;

SET @idx_activities_drop_in_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND INDEX_NAME = 'idx_activities_drop_in'
);
SET @ddl_drop_activities_drop_in := IF(
  @idx_activities_drop_in_exists > 0,
  'ALTER TABLE activities DROP INDEX idx_activities_drop_in',
  'SELECT "idx_activities_drop_in not present"'
);
PREPARE stmt_drop_activities_drop_in FROM @ddl_drop_activities_drop_in;
EXECUTE stmt_drop_activities_drop_in;
DEALLOCATE PREPARE stmt_drop_activities_drop_in;
