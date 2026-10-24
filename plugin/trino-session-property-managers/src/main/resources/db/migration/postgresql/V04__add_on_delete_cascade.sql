ALTER TABLE session_client_tags
    DROP CONSTRAINT session_client_tags_tag_spec_id_fkey,
    ADD CONSTRAINT session_client_tags_spec_id_fk
        FOREIGN KEY (tag_spec_id) REFERENCES session_specs (spec_id) ON DELETE CASCADE;
ALTER TABLE session_property_values
    DROP CONSTRAINT session_property_values_property_spec_id_fkey,
    ADD CONSTRAINT session_property_values_spec_id_fk
        FOREIGN KEY (property_spec_id) REFERENCES session_specs (spec_id) ON DELETE CASCADE;
