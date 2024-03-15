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
package io.trino.plugin.deltalake.kernel;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.kernel.engine.TrinoEngine;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.kernel.KernelSchemaUtils.toTrinoType;

public class KernelClient
{
    private KernelClient() {}

    private static final Map<SnapshotCacheKey, Snapshot> snapshotCache = new ConcurrentHashMap<>();

    public static Engine createEngine(TrinoFileSystem fileSystem, TypeManager typeManager)
    {
        return new TrinoEngine(fileSystem, typeManager);
    }

    public static Scan createScan(Engine engine, KernelDeltaLakeTableHandle tableHandle)
    {
        return tableHandle.getDeltaSnapshot().orElseThrow()
                .getScanBuilder(engine)
                // TODO: extract projects and filters from tableHandle
                .build();
    }

    public static Optional<Snapshot> getSnapshot(Engine engine, String tableLocation)
    {
        try {
            Table deltaTable = Table.forPath(engine, tableLocation);
            return Optional.of(deltaTable.getLatestSnapshot(engine));
        }
        catch (Exception e) {
            // TODO: Log
            return Optional.empty();
        }
    }

    public static List<ColumnMetadata> getTableColumnMetadata(
            Engine engine,
            TypeManager typeManager,
            SchemaTableName tableName,
            Snapshot snapshot)
    {
        StructType schema = snapshot.getSchema(engine);
        return schema.fields().stream()
                .map(field ->
                        new ColumnMetadata(
                                field.getName(),
                                toTrinoType(tableName, typeManager, field.getDataType())))
                .collect(toImmutableList());
    }

    public static long getVersion(Engine engine, Snapshot snapshot)
    {
        return snapshot.getVersion(engine);
    }

    public static StructType getSchema(Engine engine, Snapshot snapshot)
    {
        return snapshot.getSchema(engine);
    }

    static class SnapshotCacheKey
    {
        private final String tableLocation;
        private final long snapshotVersion;

        public SnapshotCacheKey(String tableLocation, long snapshotVersion)
        {
            this.tableLocation = tableLocation;
            this.snapshotVersion = snapshotVersion;
        }

        public String getTableLocation()
        {
            return tableLocation;
        }

        public long getSnapshotVersion()
        {
            return snapshotVersion;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotCacheKey that = (SnapshotCacheKey) o;
            return snapshotVersion == that.snapshotVersion && Objects.equals(tableLocation, that.tableLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, snapshotVersion);
        }
    }
}
