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
package io.trino.metadata;

import io.trino.sql.tree.Identifier;

import static java.util.Locale.ENGLISH;

public interface Canonicalizer
{
    Canonicalizer LEGACY_CANONICALIZER = value -> value.toLowerCase(ENGLISH);
    Canonicalizer LOWERCASE_CANONICALIZER = value -> value.toLowerCase(ENGLISH);

    String canonicalize(String value);

    default String canonicalize(String value, boolean delimited)
    {
        return delimited ? value : canonicalize(value);
    }

    default String canonicalize(Identifier identifier)
    {
        return canonicalize(identifier.getValue(), identifier.isDelimited());
    }

    default String canonicalizeColumn(String value)
    {
        return canonicalize(value);
    }

    default String canonicalizeColumn(String value, boolean delimited)
    {
        return canonicalize(value, delimited);
    }

    default String canonicalizeColumn(Identifier identifier)
    {
        return canonicalizeColumn(identifier.getValue(), identifier.isDelimited());
    }

    default String canonicalizeField(String value)
    {
        return canonicalize(value);
    }

    default String compareColumn(String value)
    {
        return value;
    }

    default boolean requiresDelimiter(String value)
    {
        return !Identifier.isValidIdentifier(value) || !canonicalize(value).equals(value);
    }

    default boolean requiresColumnDelimiter(String value)
    {
        return requiresDelimiter(value);
    }

    default String schemaMetadata(String value)
    {
        return value;
    }

    default String tableMetadata(String value)
    {
        return value;
    }

    default String columnMetadata(String value)
    {
        return value;
    }
}
