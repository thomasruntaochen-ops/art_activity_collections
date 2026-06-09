ALTER TABLE activities
  ADD COLUMN audience_segment ENUM('kids','teens','adults','all_ages','unknown') NOT NULL DEFAULT 'unknown' AFTER age_max;

CREATE INDEX idx_activities_status_audience_start_at
  ON activities (status, audience_segment, start_at);
