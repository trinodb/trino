CREATE TABLE resource_groups (
    resource_group_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
    name VARCHAR(250) NOT NULL,
    soft_memory_limit VARCHAR(128) NOT NULL,
    max_queued INT NOT NULL,
    soft_concurrency_limit NUMBER,
    hard_concurrency_limit NUMBER NOT NULL,
    scheduling_policy VARCHAR(128),
    scheduling_weight NUMBER,
    jmx_export CHAR(1),
    soft_cpu_limit VARCHAR(128),
    hard_cpu_limit VARCHAR(128),
    parent NUMBER,
    environment VARCHAR(128),
    PRIMARY KEY(resource_group_id),
    FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE
);
