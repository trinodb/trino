CREATE TABLE selectors (
     resource_group_id NUMBER NOT NULL,
     priority NUMBER NOT NULL,
     user_regex VARCHAR(512),
     source_regex VARCHAR(512),
     query_type VARCHAR(512),
     client_tags VARCHAR(512),
     selector_resource_estimate VARCHAR(1024),
     FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE
);
