package io.trino.plugin.couchbase;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

public record NamedParametrizedString(@Nullable String name, @Nonnull ParametrizedString value) {
    @Nonnull
    @Override
    public String toString() {
        if (name == null) {
            return value.toString();
        } else {
            return String.format("%s `%s`", value, name);
        }
    }
}
