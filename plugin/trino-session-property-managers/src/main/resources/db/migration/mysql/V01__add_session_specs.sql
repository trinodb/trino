CREATE TABLE IF NOT EXISTS session_specs (
    spec_id BIGINT NOT NULL AUTO_INCREMENT,
    user_regex VARCHAR(512),
    source_regex VARCHAR(512),
    query_type VARCHAR(512),
    group_regex VARCHAR(512),
    priority INT NOT NULL,
    PRIMARY KEY (spec_id)
);
