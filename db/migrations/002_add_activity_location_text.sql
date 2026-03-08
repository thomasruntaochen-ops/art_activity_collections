SET @col_exists := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activities'
    AND COLUMN_NAME = 'location_text'
);

SET @ddl := IF(
  @col_exists = 0,
  'ALTER TABLE activities ADD COLUMN location_text VARCHAR(512) NULL AFTER recurrence_text',
  'SELECT \"location_text already exists\"'
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
