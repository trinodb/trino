CREATE TABLE IF NOT EXISTS resource_groups (
    resource_group_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(250) NOT NULL,
    soft_memory_limit VARCHAR(128) NOT NULL,
    max_queued INT NOT NULL,
    soft_concurrency_limit INT,
    hard_concurrency_limit INT NOT NULL,
    scheduling_policy VARCHAR(128),
    scheduling_weight INT,
    jmx_export BOOLEAN,
    soft_cpu_limit VARCHAR(128),
    hard_cpu_limit VARCHAR(128),
    parent BIGINT,
    environment VARCHAR(128),
    FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE
);
