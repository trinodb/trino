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
package io.trino.plugin.jdbc;

import io.airlift.slice.SizeOf;

import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record JdbcTypeHandle(
        int jdbcType,
        Optional<String> jdbcTypeName,
        Optional<Integer> columnSize,
        Optional<Integer> decimalDigits,
        Optional<Integer> arrayDimensions,
        Optional<CaseSensitivity> caseSensitivity)
{
    private static final int INSTANCE_SIZE = instanceSize(JdbcTypeHandle.class);

    public JdbcTypeHandle
    {
        requireNonNull(jdbcTypeName, "jdbcTypeName is null");
        requireNonNull(columnSize, "columnSize is null");
        requireNonNull(decimalDigits, "decimalDigits is null");
        requireNonNull(arrayDimensions, "arrayDimensions is null");
        requireNonNull(caseSensitivity, "caseSensitivity is null");
    }

    public int requiredColumnSize()
    {
        return columnSize().orElseThrow(() -> new IllegalStateException("column size not present"));
    }

    public int requiredDecimalDigits()
    {
        return decimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(jdbcType)
                + sizeOf(jdbcTypeName, SizeOf::estimatedSizeOf)
                + sizeOf(columnSize, SizeOf::sizeOf)
                + sizeOf(decimalDigits, SizeOf::sizeOf)
                + sizeOf(arrayDimensions, SizeOf::sizeOf)
                + sizeOf(caseSensitivity, ignored -> 0);
    }
}
