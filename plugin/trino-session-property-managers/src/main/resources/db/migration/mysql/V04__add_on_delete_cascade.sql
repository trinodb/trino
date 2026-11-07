ALTER TABLE session_client_tags DROP FOREIGN KEY session_client_tags_ibfk_1;
ALTER TABLE session_client_tags ADD CONSTRAINT session_client_tags_spec_id_fk
    FOREIGN KEY (tag_spec_id) REFERENCES session_specs (spec_id) ON DELETE CASCADE;
ALTER TABLE session_property_values DROP FOREIGN KEY session_property_values_ibfk_1;
ALTER TABLE session_property_values ADD CONSTRAINT session_property_values_spec_id_fk
    FOREIGN KEY (property_spec_id) REFERENCES session_specs (spec_id) ON DELETE CASCADE;
