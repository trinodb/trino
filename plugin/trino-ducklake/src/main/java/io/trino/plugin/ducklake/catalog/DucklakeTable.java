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
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Represents a table from the ducklake_table table.
 */
public record DucklakeTable(
        long tableId,
        UUID tableUuid,
        long beginSnapshot,
        Optional<Long> endSnapshot,
        long schemaId,
        String tableName,
        Optional<String> path,
        Optional<Boolean> pathIsRelative)
{
    public DucklakeTable
    {
        requireNonNull(tableUuid, "tableUuid is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(endSnapshot, "endSnapshot is null");
        requireNonNull(path, "path is null");
        requireNonNull(pathIsRelative, "pathIsRelative is null");
    }
}
