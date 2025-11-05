ALTER TABLE selectors
    ADD COLUMN original_user_regex VARCHAR(512),
    ADD COLUMN authenticated_user_regex VARCHAR(512);
