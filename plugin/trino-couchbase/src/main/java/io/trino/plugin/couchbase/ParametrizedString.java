package io.trino.plugin.couchbase;

import io.trino.spi.connector.ColumnHandle;
import jakarta.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public record ParametrizedString(@Nonnull String text, List<Object> params) {

    public static ParametrizedString from(String text) {
        return new ParametrizedString(text, Collections.EMPTY_LIST);
    }

    public static ParametrizedString join(List<ParametrizedString> others, String delimeter, String before, String after) {
        List<Object> params = new ArrayList<>();
        if (others.size() == 1) {
            return others.get(0);
        }
        return new ParametrizedString(
                others.stream()
                        .peek(ps -> {
                            params.addAll(ps.params());
                        })
                        .map(ParametrizedString::toString)
                        .collect(Collectors.joining(delimeter, before, after)),
                params
        );
    }

    public static ParametrizedString from(String s, List<Object> params) {
        return new ParametrizedString(
                s, params
        );
    }

    @Nonnull
    @Override
    public String toString() {
        return text;
    }
}
