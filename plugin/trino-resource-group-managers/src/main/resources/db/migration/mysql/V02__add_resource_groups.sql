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
