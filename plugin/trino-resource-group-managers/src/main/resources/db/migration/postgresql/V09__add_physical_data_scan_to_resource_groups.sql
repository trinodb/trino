ALTER TABLE resource_groups ADD COLUMN hard_physical_data_scan_limit VARCHAR(128);

-- Find and drop the old constraint dynamically
DO $$
DECLARE
    constraint_name_to_drop TEXT;
BEGIN
    SELECT conname
    INTO constraint_name_to_drop
    FROM pg_constraint
    WHERE conrelid = 'resource_groups_global_properties'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%cpu_quota_period%';

    IF constraint_name_to_drop IS NOT NULL THEN
            EXECUTE 'ALTER TABLE resource_groups_global_properties DROP CONSTRAINT ' || quote_ident(constraint_name_to_drop);
    END IF;
END;
$$;

-- Add the new CHECK constraint with an explicit and permanent name.
ALTER TABLE resource_groups_global_properties ADD CONSTRAINT resource_groups_global_properties_name_check CHECK (name IN ('cpu_quota_period', 'physical_data_scan_quota_period'));
