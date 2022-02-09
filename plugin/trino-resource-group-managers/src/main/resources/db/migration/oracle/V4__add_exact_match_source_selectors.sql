CREATE TABLE exact_match_source_selectors(
     environment VARCHAR(128),
     source VARCHAR(512) NOT NULL,
     query_type VARCHAR(512),
     update_time TIMESTAMP NOT NULL,
     resource_group_id VARCHAR(256) NOT NULL,
     PRIMARY KEY (environment, source, resource_group_id),
     UNIQUE (source, environment, query_type, resource_group_id)
);
