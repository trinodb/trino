CREATE TABLE IF NOT EXISTS resource_groups_global_properties (
    name VARCHAR(128) NOT NULL PRIMARY KEY,
    value VARCHAR(512) NULL,
    CHECK (name in ('cpu_quota_period'))
);

CREATE TABLE IF NOT EXISTS resource_groups (
    resource_group_id BIGINT NOT NULL AUTO_INCREMENT,
    name VARCHAR(250) NOT NULL,
    soft_memory_limit VARCHAR(128) NOT NULL,
    max_queued INT NOT NULL,
    soft_concurrency_limit INT NULL,
    hard_concurrency_limit INT NOT NULL,
    scheduling_policy VARCHAR(128) NULL,
    scheduling_weight INT NULL,
    jmx_export BOOLEAN NULL,
    soft_cpu_limit VARCHAR(128) NULL,
    hard_cpu_limit VARCHAR(128) NULL,
    parent BIGINT NULL,
    environment VARCHAR(128) NULL,
    PRIMARY KEY (resource_group_id),
    FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS selectors (
     resource_group_id BIGINT NOT NULL,
     priority BIGINT NOT NULL,
     user_regex VARCHAR(512),
     source_regex VARCHAR(512),
     query_type VARCHAR(512),
     client_tags VARCHAR(512),
     selector_resource_estimate VARCHAR(1024),
     FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS exact_match_source_selectors(
     environment VARCHAR(128),
     source VARCHAR(512) NOT NULL,
     query_type VARCHAR(512),
     update_time DATETIME NOT NULL,
     resource_group_id VARCHAR(256) NOT NULL,
     PRIMARY KEY (environment, source(128), resource_group_id),
     UNIQUE (source(128), environment, query_type(128), resource_group_id)
);

