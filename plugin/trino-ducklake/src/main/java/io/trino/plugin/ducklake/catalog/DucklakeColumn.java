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
package io.trino.plugin.ducklake.catalog;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a column from the ducklake_column table.
 */
public record DucklakeColumn(
        long columnId,
        long beginSnapshot,
        Optional<Long> endSnapshot,
        long tableId,
        long columnOrder,
        String columnName,
        String columnType,
        boolean nullsAllowed,
        Optional<Long> parentColumn)
{
    public DucklakeColumn
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
        requireNonNull(endSnapshot, "endSnapshot is null");
        requireNonNull(parentColumn, "parentColumn is null");
    }
}
