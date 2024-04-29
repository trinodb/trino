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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StatisticsUpdateMode;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveMetadataListing
        extends AbstractTestQueryFramework
{
    private static final String DATABASE_NAME = "database";
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"),
            ImmutableMap.of());
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            Optional.of("location"),
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), 10, ImmutableList.of(new SortingColumn("column", SortingColumn.Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));

    private static final Table CORRECT_VIEW = new Table(
            DATABASE_NAME,
            "correct_view",
            Optional.of("owner"),
            "VIRTUAL_VIEW",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("PRESTO_VIEW_FLAG", "value3"),
            Optional.of("SELECT 1"),
            Optional.of("SELECT 1"),
            OptionalLong.empty());

    private static final Table FAILING_STORAGE_DESCRIPTOR_VIEW = new Table(
            DATABASE_NAME,
            "failing_storage_descriptor_view",
            Optional.of("owner"),
            "VIRTUAL_VIEW",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("PRESTO_VIEW_FLAG", "value3"),
            Optional.of("SELECT 1"),
            Optional.of("SELECT 1"),
            OptionalLong.empty());

    private static final Table FAILING_GENERAL_VIEW = new Table(
            DATABASE_NAME,
            "failing_general_view",
            Optional.of("owner"),
            "VIRTUAL_VIEW",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("PRESTO_VIEW_FLAG", "value3"),
            Optional.of("SELECT 1"),
            Optional.of("SELECT 1"),
            OptionalLong.empty());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setCreateTpchSchemas(false)
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperty("hive.hive-views.enabled", "true")
                .setMetastore(runner -> new TestingHiveMetastore())
                .build();
    }

    @Test
    public void testViewListing()
    {
        String withNoFilter = "SELECT table_name FROM information_schema.views";
        assertThat(computeScalar(withNoFilter)).isEqualTo(CORRECT_VIEW.getSchemaTableName().getTableName());

        String withSchemaFilter = format("SELECT table_name FROM information_schema.views WHERE table_schema = '%s'", DATABASE_NAME);
        assertThat(computeScalar(withSchemaFilter)).isEqualTo(CORRECT_VIEW.getSchemaTableName().getTableName());

        String withSchemaAndCorrectViewFilter = format(
                "SELECT table_name FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s'",
                DATABASE_NAME,
                CORRECT_VIEW.getSchemaTableName().getTableName());
        assertThat(computeScalar(withSchemaAndCorrectViewFilter)).isEqualTo(CORRECT_VIEW.getSchemaTableName().getTableName());

        String withSchemaAndFailingSDViewFilter = format(
                "SELECT table_name FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s'",
                DATABASE_NAME,
                FAILING_STORAGE_DESCRIPTOR_VIEW.getSchemaTableName().getTableName());
        assertQueryReturnsEmptyResult(withSchemaAndFailingSDViewFilter);

        String withSchemaAndFailingGeneralViewFilter = format(
                "SELECT table_name FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s'",
                DATABASE_NAME,
                FAILING_GENERAL_VIEW.getSchemaTableName().getTableName());
        assertQueryReturnsEmptyResult(withSchemaAndFailingGeneralViewFilter);
    }

    private static class TestingHiveMetastore
            implements HiveMetastore
    {
        @Override
        public List<String> getAllDatabases()
        {
            return ImmutableList.of(DATABASE_NAME);
        }

        @Override
        public List<TableInfo> getTables(String databaseName)
        {
            return ImmutableList.<TableInfo>builder()
                    .add(new TableInfo(CORRECT_VIEW.getSchemaTableName(), TableInfo.ExtendedRelationType.OTHER_VIEW))
                    .add(new TableInfo(FAILING_STORAGE_DESCRIPTOR_VIEW.getSchemaTableName(), TableInfo.ExtendedRelationType.OTHER_VIEW))
                    .add(new TableInfo(FAILING_GENERAL_VIEW.getSchemaTableName(), TableInfo.ExtendedRelationType.OTHER_VIEW))
                    .build();
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
            if (schemaTableName.equals(CORRECT_VIEW.getSchemaTableName())) {
                return Optional.of(CORRECT_VIEW);
            }
            if (schemaTableName.equals(FAILING_STORAGE_DESCRIPTOR_VIEW.getSchemaTableName())) {
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Table StorageDescriptor is null for failing_view");
            }
            if (schemaTableName.equals(FAILING_GENERAL_VIEW.getSchemaTableName())) {
                throw new RuntimeException("General error");
            }
            return Optional.empty();
        }

        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDatabase(Database database)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase(String databaseName, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDatabase(String databaseName, String newDatabaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDatabaseOwner(String databaseName, HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(Table table, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(String databaseName, String tableName, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commentTable(String databaseName, String tableName, Optional<String> comment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropColumn(String databaseName, String tableName, String columnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Partition> getPartition(Table table, List<String> partitionValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createRole(String role, String grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropRole(String role)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> listRoles()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean functionExists(String databaseName, String functionName, String signatureToken)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<LanguageFunction> getAllFunctions(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createFunction(String databaseName, String functionName, LanguageFunction function)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropFunction(String databaseName, String functionName, String signatureToken)
        {
            throw new UnsupportedOperationException();
        }
    }
}
