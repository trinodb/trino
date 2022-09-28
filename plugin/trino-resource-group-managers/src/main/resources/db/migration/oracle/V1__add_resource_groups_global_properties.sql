CREATE TABLE resource_groups_global_properties (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    value VARCHAR(512) NULL,
    CHECK (name in ('cpu_quota_period'))
);
