CREATE TABLE IF NOT EXISTS session_property_values (
    property_spec_id BIGINT NOT NULL,
    session_property_name VARCHAR(512),
    session_property_value VARCHAR(512),
    PRIMARY KEY (property_spec_id, session_property_name),
    FOREIGN KEY (property_spec_id) REFERENCES session_specs (spec_id)
);
