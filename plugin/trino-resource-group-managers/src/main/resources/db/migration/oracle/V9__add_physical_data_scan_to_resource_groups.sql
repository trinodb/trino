ALTER TABLE resource_groups ADD hard_physical_data_scan_limit VARCHAR(128);

-- Find and drop the old constraint dynamically
BEGIN
    FOR c IN (
       SELECT constraint_name
       FROM user_constraints
       WHERE table_name = 'RESOURCE_GROUPS_GLOBAL_PROPERTIES'
         AND constraint_type = 'C'
         AND search_condition_vc LIKE '%cpu_quota_period%'
   ) LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE resource_groups_global_properties DROP CONSTRAINT ' || c.constraint_name;
    END LOOP;
END;
/

-- Add the new CHECK constraint with an explicit and permanent name.
ALTER TABLE resource_groups_global_properties ADD CONSTRAINT resource_groups_global_properties_name_check CHECK (name IN ('cpu_quota_period', 'physical_data_scan_quota_period'));
