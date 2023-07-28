/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.intersection;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QueryDataEncodings
{
    private final List<Encoding> encodings;

    private static final Splitter SPLITTER = Splitter.on(";");
    private static final Joiner JOINER = Joiner.on(";");

    private QueryDataEncodings(List<Encoding> encodings)
    {
        this.encodings = ImmutableList.copyOf(requireNonNull(encodings, "encodings is null"));
    }

    public QueryDataEncodings enforceSingleEncoding()
    {
        verify(encodings.size() == 1, "Single encoding was expected but got: %s", this);
        return this;
    }

    public QueryDataEncodings and(QueryDataEncodings selector)
    {
        return new QueryDataEncodings(ImmutableList.<Encoding>builder().addAll(this.encodings).addAll(selector.encodings).build());
    }

    public boolean matchesAny(QueryDataEncodings encodings)
    {
        return !intersection(ImmutableSet.copyOf(this.encodings), ImmutableSet.copyOf(encodings.encodings)).isEmpty();
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryDataEncodings)) {
            return false;
        }

        QueryDataEncodings selector = (QueryDataEncodings) o;
        return Objects.equals(encodings, selector.encodings);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(encodings);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return JOINER.join(encodings);
    }

    public static QueryDataEncodings singleEncoding(QueryDataReference reference, QueryDataSerialization serialization)
    {
        return new QueryDataEncodings(ImmutableList.of(new Encoding(reference, serialization)));
    }

    public static QueryDataEncodings multipleEncodings(List<QueryDataEncodings> encodings)
    {
        return new QueryDataEncodings(encodings.stream().flatMap(encoding -> encoding.encodings.stream()).collect(toImmutableList()));
    }

    @JsonCreator
    public static QueryDataEncodings parseEncodings(String value)
    {
        return new QueryDataEncodings(SPLITTER.splitToStream(value).map(Encoding::parseEncoding).collect(toImmutableList()));
    }

    private static class Encoding
    {
        private final QueryDataSerialization serialization;
        private final QueryDataReference reference;

        @JsonCreator
        public static Encoding parseEncoding(String value)
        {
            String[] split = value.split("-", 2);
            if (split.length == 2) {
                return new Encoding(
                        QueryDataReference.valueOf(split[0].toUpperCase(ENGLISH)),
                        QueryDataSerialization.valueOf(split[1].toUpperCase(ENGLISH)));
            }
            throw new IllegalArgumentException("QueryDataEncodings.Encoding has unsupported format: " + value);
        }

        Encoding(QueryDataReference reference, QueryDataSerialization serialization)
        {
            this.reference = requireNonNull(reference, "reference is null");
            this.serialization = requireNonNull(serialization, "serialization is null");
        }

        @Override
        @JsonValue
        public String toString()
        {
            return reference.name().toLowerCase(ENGLISH) + "-" + serialization.toString().toLowerCase(ENGLISH);
        }

        @Override
        public final boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Encoding)) {
                return false;
            }

            Encoding entry = (Encoding) o;
            return reference == entry.reference && serialization == entry.serialization;
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hashCode(reference);
            result = 31 * result + Objects.hashCode(serialization);
            return result;
        }
    }
}
