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

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * Represents a snapshot from the ducklake_snapshot table.
 */
public record DucklakeSnapshot(
        long snapshotId,
        Instant snapshotTime,
        long schemaVersion,
        long nextCatalogId,
        long nextFileId)
{
    public DucklakeSnapshot
    {
        requireNonNull(snapshotTime, "snapshotTime is null");
    }
}
