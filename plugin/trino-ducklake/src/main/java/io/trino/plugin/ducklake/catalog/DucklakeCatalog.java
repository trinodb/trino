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

import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for Ducklake SQL-based catalog operations.
 * Abstracts access to the 28 Ducklake metadata tables.
 */
public interface DucklakeCatalog
{
    /**
     * Get the current (maximum) snapshot ID
     */
    long getCurrentSnapshotId();

    /**
     * Get a specific snapshot ID (for time travel queries)
     */
    Optional<DucklakeSnapshot> getSnapshot(long snapshotId);

    /**
     * List all schemas visible at the given snapshot
     */
    List<DucklakeSchema> listSchemas(long snapshotId);

    /**
     * Get a specific schema by name at the given snapshot
     */
    Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId);

    /**
     * List all tables in a schema at the given snapshot
     */
    List<DucklakeTable> listTables(long schemaId, long snapshotId);

    /**
     * Get a specific table by name at the given snapshot
     */
    Optional<DucklakeTable> getTable(SchemaTableName tableName, long snapshotId);

    /**
     * Get table by ID at the given snapshot
     */
    Optional<DucklakeTable> getTableById(long tableId, long snapshotId);

    /**
     * Get columns for a table at the given snapshot
     */
    List<DucklakeColumn> getTableColumns(long tableId, long snapshotId);

    /**
     * Get data files for a table at the given snapshot
     */
    List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId);

    /**
     * Get file-level column statistics for predicate pushdown
     */
    List<Long> getDataFileIdsForPredicate(long tableId, long columnId, long snapshotId, Object minValue, Object maxValue);

    /**
     * Get table-level statistics (record count, file size) from ducklake_table_stats
     */
    Optional<DucklakeTableStats> getTableStats(long tableId);

    /**
     * Get aggregated column statistics across all active data files
     */
    List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId);

    /**
     * Get partition specs for a table at the given snapshot
     */
    List<DucklakePartitionSpec> getPartitionSpecs(long tableId, long snapshotId);

    /**
     * Get partition values for all active data files of a table at the given snapshot
     */
    Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId);

    /**
     * Get the base data path from ducklake_metadata
     */
    Optional<String> getDataPath();

    /**
     * Close any resources (JDBC connections, etc)
     */
    void close();
}
