ALTER TABLE resource_groups ADD COLUMN hard_physical_data_scan_limit VARCHAR(128);

-- Find and drop the old constraint dynamically
SET @constraint_name = (
    SELECT tc.CONSTRAINT_NAME
    FROM information_schema.TABLE_CONSTRAINTS AS tc
    JOIN information_schema.CHECK_CONSTRAINTS AS cc
      ON tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
      AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
    WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
      AND tc.TABLE_NAME = 'resource_groups_global_properties'
      AND tc.CONSTRAINT_TYPE = 'CHECK'
      AND cc.CHECK_CLAUSE LIKE '%cpu_quota_period%'
);

-- Drop the constraint if it exists otherwise run no-op SQL
SET @sql = IF(@constraint_name IS NOT NULL,
              CONCAT('ALTER TABLE resource_groups_global_properties DROP CHECK `', @constraint_name, '`'),
              'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add the new CHECK constraint with an explicit and permanent name.
ALTER TABLE resource_groups_global_properties ADD CONSTRAINT resource_groups_global_properties_name_check CHECK (name IN ('cpu_quota_period', 'physical_data_scan_quota_period'));
