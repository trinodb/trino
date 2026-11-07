CREATE TABLE IF NOT EXISTS session_client_tags (
    tag_spec_id BIGINT NOT NULL,
    client_tag VARCHAR(512) NOT NULL,
    PRIMARY KEY (tag_spec_id, client_tag),
    FOREIGN KEY (tag_spec_id) REFERENCES session_specs (spec_id)
);
