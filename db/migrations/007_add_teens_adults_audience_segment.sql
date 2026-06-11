ALTER TABLE activities
  MODIFY COLUMN audience_segment ENUM('kids','teens','teens_adults','adults','all_ages','unknown') NOT NULL DEFAULT 'unknown';
