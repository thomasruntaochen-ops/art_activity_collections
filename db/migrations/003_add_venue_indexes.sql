SET @idx_name_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'venues'
    AND INDEX_NAME = 'idx_venues_name'
);
SET @ddl_name := IF(
  @idx_name_exists = 0,
  'ALTER TABLE venues ADD INDEX idx_venues_name (name)',
  'SELECT "idx_venues_name already exists"'
);
PREPARE stmt_name FROM @ddl_name;
EXECUTE stmt_name;
DEALLOCATE PREPARE stmt_name;

SET @idx_city_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'venues'
    AND INDEX_NAME = 'idx_venues_city'
);
SET @ddl_city := IF(
  @idx_city_exists = 0,
  'ALTER TABLE venues ADD INDEX idx_venues_city (city)',
  'SELECT "idx_venues_city already exists"'
);
PREPARE stmt_city FROM @ddl_city;
EXECUTE stmt_city;
DEALLOCATE PREPARE stmt_city;

SET @idx_state_exists := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'venues'
    AND INDEX_NAME = 'idx_venues_state'
);
SET @ddl_state := IF(
  @idx_state_exists = 0,
  'ALTER TABLE venues ADD INDEX idx_venues_state (state)',
  'SELECT "idx_venues_state already exists"'
);
PREPARE stmt_state FROM @ddl_state;
EXECUTE stmt_state;
DEALLOCATE PREPARE stmt_state;
