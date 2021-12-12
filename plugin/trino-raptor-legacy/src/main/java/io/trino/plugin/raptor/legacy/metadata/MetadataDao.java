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
package io.trino.plugin.raptor.legacy.metadata;

import io.trino.plugin.raptor.legacy.metadata.Table.TableMapper;
import io.trino.spi.connector.SchemaTableName;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Set;

@RegisterConstructorMapper(ColumnMetadataRow.class)
@RegisterConstructorMapper(TableMetadataRow.class)
@RegisterConstructorMapper(TableStatsRow.class)
@RegisterRowMapper(SchemaTableNameMapper.class)
@RegisterRowMapper(TableMapper.class)
@RegisterRowMapper(ViewResult.Mapper.class)
public interface MetadataDao
{
    String TABLE_INFORMATION_SELECT = "" +
            "SELECT t.table_id, t.distribution_id, d.distribution_name, d.bucket_count, t.temporal_column_id, t.organization_enabled\n" +
            "FROM tables t\n" +
            "LEFT JOIN distributions d ON (t.distribution_id = d.distribution_id)\n";

    String TABLE_COLUMN_SELECT = "" +
            "SELECT t.schema_name, t.table_name,\n" +
            "  c.column_id, c.column_name, c.data_type, c.ordinal_position,\n" +
            "  c.bucket_ordinal_position, c.sort_ordinal_position,\n" +
            "  t.temporal_column_id = c.column_id AS temporal\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n";

    @SqlQuery(TABLE_INFORMATION_SELECT +
            "WHERE t.table_id = :tableId")
    Table getTableInformation(long tableId);

    @SqlQuery(TABLE_INFORMATION_SELECT +
            "WHERE t.schema_name = :schemaName\n" +
            "  AND t.table_name = :tableName")
    Table getTableInformation(
            String schemaName,
            String tableName);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.column_id = :columnId\n" +
            "ORDER BY c.ordinal_position\n")
    TableColumn getTableColumn(
            long tableId,
            long columnId);

    @SqlQuery("SELECT schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)")
    List<SchemaTableName> listTables(
            String schemaName);

    @SqlQuery("SELECT DISTINCT schema_name FROM tables")
    List<String> listSchemaNames();

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name, ordinal_position")
    List<TableColumn> listTableColumns(
            String schemaName,
            String tableName);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "ORDER BY c.ordinal_position")
    List<TableColumn> listTableColumns(long tableId);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.sort_ordinal_position IS NOT NULL\n" +
            "ORDER BY c.sort_ordinal_position")
    List<TableColumn> listSortColumns(long tableId);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.bucket_ordinal_position IS NOT NULL\n" +
            "ORDER BY c.bucket_ordinal_position")
    List<TableColumn> listBucketColumns(long tableId);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)")
    List<SchemaTableName> listViews(
            String schemaName);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name\n")
    List<ViewResult> getViews(
            String schemaName,
            String tableName);

    @SqlUpdate("INSERT INTO tables (\n" +
            "  schema_name, table_name, compaction_enabled, organization_enabled, distribution_id,\n" +
            "  create_time, update_time, table_version,\n" +
            "  shard_count, row_count, compressed_size, uncompressed_size)\n" +
            "VALUES (\n" +
            "  :schemaName, :tableName, :compactionEnabled, :organizationEnabled, :distributionId,\n" +
            "  :createTime, :createTime, 0,\n" +
            "  0, 0, 0, 0)\n")
    @GetGeneratedKeys
    long insertTable(
            String schemaName,
            String tableName,
            boolean compactionEnabled,
            boolean organizationEnabled,
            Long distributionId,
            long createTime);

    @SqlUpdate("UPDATE tables SET\n" +
            "  update_time = :updateTime\n" +
            ", table_version = table_version + 1\n" +
            "WHERE table_id = :tableId")
    void updateTableVersion(
            long tableId,
            long updateTime);

    @SqlUpdate("UPDATE tables SET\n" +
            "  shard_count = shard_count + :shardCount \n" +
            ", row_count = row_count + :rowCount\n" +
            ", compressed_size = compressed_size + :compressedSize\n" +
            ", uncompressed_size = uncompressed_size + :uncompressedSize\n" +
            "WHERE table_id = :tableId")
    void updateTableStats(
            long tableId,
            long shardCount,
            long rowCount,
            long compressedSize,
            long uncompressedSize);

    @SqlUpdate("INSERT INTO columns (table_id, column_id, column_name, ordinal_position, data_type, sort_ordinal_position, bucket_ordinal_position)\n" +
            "VALUES (:tableId, :columnId, :columnName, :ordinalPosition, :dataType, :sortOrdinalPosition, :bucketOrdinalPosition)")
    void insertColumn(
            long tableId,
            long columnId,
            String columnName,
            int ordinalPosition,
            String dataType,
            Integer sortOrdinalPosition,
            Integer bucketOrdinalPosition);

    @SqlUpdate("UPDATE tables SET\n" +
            "  schema_name = :newSchemaName\n" +
            ", table_name = :newTableName\n" +
            "WHERE table_id = :tableId")
    void renameTable(
            long tableId,
            String newSchemaName,
            String newTableName);

    @SqlUpdate("UPDATE columns SET column_name = :target\n" +
            "WHERE table_id = :tableId\n" +
            "  AND column_id = :columnId")
    void renameColumn(
            long tableId,
            long columnId,
            String target);

    @SqlUpdate("DELETE FROM columns\n" +
            " WHERE table_id = :tableId\n" +
            "  AND column_id = :columnId")
    void dropColumn(
            long tableId,
            long columnId);

    @SqlUpdate("INSERT INTO views (schema_name, table_name, data)\n" +
            "VALUES (:schemaName, :tableName, :data)")
    void insertView(
            String schemaName,
            String tableName,
            String data);

    @SqlUpdate("DELETE FROM tables WHERE table_id = :tableId")
    int dropTable(long tableId);

    @SqlUpdate("DELETE FROM columns WHERE table_id = :tableId")
    int dropColumns(long tableId);

    @SqlUpdate("DELETE FROM views\n" +
            "WHERE schema_name = :schemaName\n" +
            "  AND table_name = :tableName")
    int dropView(
            String schemaName,
            String tableName);

    @SqlQuery("SELECT temporal_column_id\n" +
            "FROM tables\n" +
            "WHERE table_id = :tableId")
    Long getTemporalColumnId(long tableId);

    @SqlUpdate("UPDATE tables SET\n" +
            "temporal_column_id = :columnId\n" +
            "WHERE table_id = :tableId")
    void updateTemporalColumnId(
            long tableId,
            long columnId);

    @SqlQuery("SELECT compaction_enabled AND maintenance_blocked IS NULL\n" +
            "FROM tables\n" +
            "WHERE table_id = :tableId")
    boolean isCompactionEligible(long tableId);

    @SqlQuery("SELECT table_id FROM tables WHERE table_id = :tableId FOR UPDATE")
    Long getLockedTableId(long tableId);

    @SqlQuery("SELECT distribution_id, distribution_name, column_types, bucket_count\n" +
            "FROM distributions\n" +
            "WHERE distribution_id = :distributionId")
    Distribution getDistribution(long distributionId);

    @SqlQuery("SELECT distribution_id, distribution_name, column_types, bucket_count\n" +
            "FROM distributions\n" +
            "WHERE distribution_name = :distributionName")
    Distribution getDistribution(String distributionName);

    @SqlUpdate("INSERT INTO distributions (distribution_name, column_types, bucket_count)\n" +
            "VALUES (:distributionName, :columnTypes, :bucketCount)")
    @GetGeneratedKeys
    long insertDistribution(
            String distributionName,
            String columnTypes,
            int bucketCount);

    @SqlQuery("SELECT table_id, schema_name, table_name, temporal_column_id, distribution_name, bucket_count, organization_enabled\n" +
            "FROM tables\n" +
            "LEFT JOIN distributions\n" +
            "ON tables.distribution_id = distributions.distribution_id\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY table_id")
    List<TableMetadataRow> getTableMetadataRows(
            String schemaName,
            String tableName);

    @SqlQuery("SELECT table_id, column_id, column_name, sort_ordinal_position, bucket_ordinal_position\n" +
            "FROM columns\n" +
            "WHERE table_id IN (\n" +
            "  SELECT table_id\n" +
            "  FROM tables\n" +
            "  WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "    AND (table_name = :tableName OR :tableName IS NULL))\n" +
            "ORDER BY table_id")
    List<ColumnMetadataRow> getColumnMetadataRows(
            String schemaName,
            String tableName);

    @SqlQuery("SELECT schema_name, table_name, create_time, update_time, table_version,\n" +
            "  shard_count, row_count, compressed_size, uncompressed_size\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name")
    List<TableStatsRow> getTableStatsRows(
            String schemaName,
            String tableName);

    @SqlQuery("SELECT table_id\n" +
            "FROM tables\n" +
            "WHERE organization_enabled\n" +
            "  AND maintenance_blocked IS NULL\n" +
            "  AND table_id IN\n" +
            "       (SELECT table_id\n" +
            "        FROM columns\n" +
            "        WHERE sort_ordinal_position IS NOT NULL)")
    Set<Long> getOrganizationEligibleTables();

    @SqlUpdate("UPDATE tables SET maintenance_blocked = CURRENT_TIMESTAMP\n" +
            "WHERE table_id = :tableId\n" +
            "  AND maintenance_blocked IS NULL")
    void blockMaintenance(long tableId);

    @SqlUpdate("UPDATE tables SET maintenance_blocked = NULL\n" +
            "WHERE table_id = :tableId")
    void unblockMaintenance(long tableId);

    @SqlQuery("SELECT maintenance_blocked IS NOT NULL\n" +
            "FROM tables\n" +
            "WHERE table_id = :tableId\n" +
            "FOR UPDATE")
    boolean isMaintenanceBlockedLocked(long tableId);

    @SqlUpdate("UPDATE tables SET maintenance_blocked = NULL\n" +
            "WHERE maintenance_blocked IS NOT NULL")
    void unblockAllMaintenance();
}
