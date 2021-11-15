CREATE TABLE IF NOT EXISTS exact_match_source_selectors(
     environment VARCHAR(128),
     source VARCHAR(512) NOT NULL,
     query_type VARCHAR(512),
     update_time DATETIME NOT NULL,
     resource_group_id VARCHAR(256) NOT NULL,
     PRIMARY KEY (environment, source(128), resource_group_id),
     UNIQUE (source(128), environment, query_type(128), resource_group_id)
);
