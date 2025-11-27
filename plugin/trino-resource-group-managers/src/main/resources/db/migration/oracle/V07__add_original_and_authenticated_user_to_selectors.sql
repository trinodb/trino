ALTER TABLE selectors ADD (
    original_user_regex VARCHAR(512),
    authenticated_user_regex VARCHAR(512)
);
