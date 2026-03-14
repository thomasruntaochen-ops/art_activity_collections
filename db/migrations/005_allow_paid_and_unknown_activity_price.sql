ALTER TABLE activities
  DROP CHECK chk_activities_is_free_true;

ALTER TABLE activities
  MODIFY COLUMN is_free BOOLEAN NULL DEFAULT NULL;
