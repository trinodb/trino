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
 * Represents a schema from the ducklake_schema table.
 */
public record DucklakeSchema(
        long schemaId,
        UUID schemaUuid,
        long beginSnapshot,
        Optional<Long> endSnapshot,
        String schemaName,
        Optional<String> path,
        Optional<Boolean> pathIsRelative)
{
    public DucklakeSchema
    {
        requireNonNull(schemaUuid, "schemaUuid is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(endSnapshot, "endSnapshot is null");
        requireNonNull(path, "path is null");
        requireNonNull(pathIsRelative, "pathIsRelative is null");
    }
}
