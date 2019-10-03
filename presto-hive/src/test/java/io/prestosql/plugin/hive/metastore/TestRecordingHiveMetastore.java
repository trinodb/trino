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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.prestosql.plugin.hive.metastore.SortingColumn.Order;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.prestosql.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.prestosql.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

public class TestRecordingHiveMetastore
{
    private static final Database DATABASE = new Database(
            "database",
            Optional.of("location"),
            "owner",
            USER,
            Optional.of("comment"),
            ImmutableMap.of("param", "value"));
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            "location",
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), 10, ImmutableList.of(new SortingColumn("column", Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));
    private static final Table TABLE = new Table(
            "database",
            "table",
            "owner",
            "table_type",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value3"),
            Optional.of("original_text"),
            Optional.of("expanded_text"));
    private static final Partition PARTITION = new Partition(
            "database",
            "table",
            ImmutableList.of("value"),
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value4"));
    private static final PartitionStatistics PARTITION_STATISTICS = new PartitionStatistics(
            new HiveBasicStatistics(10, 11, 10000, 10001),
            ImmutableMap.of("column", new HiveColumnStatistics(
                    Optional.of(new IntegerStatistics(
                            OptionalLong.of(-100),
                            OptionalLong.of(102))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(1234),
                    OptionalLong.of(1235),
                    OptionalLong.of(1),
                    OptionalLong.of(8))));
    private static final HivePrivilegeInfo PRIVILEGE_INFO = new HivePrivilegeInfo(HivePrivilege.SELECT, true, new HivePrincipal(USER, "grantor"), new HivePrincipal(USER, "grantee"));
    private static final RoleGrant ROLE_GRANT = new RoleGrant(new PrestoPrincipal(USER, "grantee"), "role", true);
    private static final HiveIdentity HIVE_CONTEXT = new HiveIdentity(SESSION);

    @Test
    public void testRecordingHiveMetastore()
            throws IOException
    {
        HiveConfig recordingHiveConfig = new HiveConfig()
                .setRecordingPath(File.createTempFile("recording_test", "json").getAbsolutePath())
                .setRecordingDuration(new Duration(10, TimeUnit.MINUTES));

        RecordingHiveMetastore recordingHiveMetastore = new RecordingHiveMetastore(new TestingHiveMetastore(), recordingHiveConfig);
        validateMetadata(recordingHiveMetastore);
        recordingHiveMetastore.dropDatabase(HIVE_CONTEXT, "other_database");
        recordingHiveMetastore.writeRecording();

        HiveConfig replayingHiveConfig = recordingHiveConfig
                .setReplay(true);

        recordingHiveMetastore = new RecordingHiveMetastore(new UnimplementedHiveMetastore(), replayingHiveConfig);
        recordingHiveMetastore.loadRecording();
        validateMetadata(recordingHiveMetastore);
    }

    private void validateMetadata(HiveMetastore hiveMetastore)
    {
        assertEquals(hiveMetastore.getDatabase("database"), Optional.of(DATABASE));
        assertEquals(hiveMetastore.getAllDatabases(), ImmutableList.of("database"));
        assertEquals(hiveMetastore.getTable(HIVE_CONTEXT, "database", "table"), Optional.of(TABLE));
        assertEquals(hiveMetastore.getSupportedColumnStatistics(createVarcharType(123)), ImmutableSet.of(MIN_VALUE, MAX_VALUE));
        assertEquals(hiveMetastore.getTableStatistics(HIVE_CONTEXT, "database", "table"), PARTITION_STATISTICS);
        assertEquals(hiveMetastore.getPartitionStatistics(HIVE_CONTEXT, "database", "table", ImmutableSet.of("value")), ImmutableMap.of("value", PARTITION_STATISTICS));
        assertEquals(hiveMetastore.getAllTables("database"), ImmutableList.of("table"));
        assertEquals(hiveMetastore.getTablesWithParameter("database", "param", "value3"), ImmutableList.of("table"));
        assertEquals(hiveMetastore.getAllViews("database"), ImmutableList.of());
        assertEquals(hiveMetastore.getPartition(HIVE_CONTEXT, "database", "table", ImmutableList.of("value")), Optional.of(PARTITION));
        assertEquals(hiveMetastore.getPartitionNames(HIVE_CONTEXT, "database", "table"), Optional.of(ImmutableList.of("value")));
        assertEquals(hiveMetastore.getPartitionNamesByParts(HIVE_CONTEXT, "database", "table", ImmutableList.of("value")), Optional.of(ImmutableList.of("value")));
        assertEquals(hiveMetastore.getPartitionsByNames(HIVE_CONTEXT, "database", "table", ImmutableList.of("value")), ImmutableMap.of("value", Optional.of(PARTITION)));
        assertEquals(hiveMetastore.listTablePrivileges("database", "table", "owner", new HivePrincipal(USER, "user")), ImmutableSet.of(PRIVILEGE_INFO));
        assertEquals(hiveMetastore.listRoles(), ImmutableSet.of("role"));
        assertEquals(hiveMetastore.listRoleGrants(new HivePrincipal(USER, "user")), ImmutableSet.of(ROLE_GRANT));
    }

    private static class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(DATABASE);
            }

            return Optional.empty();
        }

        @Override
        public List<String> getAllDatabases()
        {
            return ImmutableList.of("database");
        }

        @Override
        public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(TABLE);
            }

            return Optional.empty();
        }

        @Override
        public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
        {
            if (type.equals(createVarcharType(123))) {
                return ImmutableSet.of(MIN_VALUE, MAX_VALUE);
            }

            return ImmutableSet.of();
        }

        @Override
        public PartitionStatistics getTableStatistics(HiveIdentity identity, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return PARTITION_STATISTICS;
            }

            return new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        @Override
        public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, String databaseName, String tableName, Set<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", PARTITION_STATISTICS);
            }

            return ImmutableMap.of();
        }

        @Override
        public List<String> getAllTables(String databaseName)
        {
            if (databaseName.equals("database")) {
                return ImmutableList.of("table");
            }

            return ImmutableList.of();
        }

        @Override
        public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
        {
            if (databaseName.equals("database") && parameterKey.equals("param") && parameterValue.equals("value3")) {
                return ImmutableList.of("table");
            }
            return ImmutableList.of();
        }

        @Override
        public List<String> getAllViews(String databaseName)
        {
            return ImmutableList.of();
        }

        @Override
        public void dropDatabase(HiveIdentity identity, String databaseName)
        {
            // noop for test purpose
        }

        @Override
        public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionValues.equals(ImmutableList.of("value"))) {
                return Optional.of(PARTITION);
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
        {
            if (databaseName.equals("database") && tableName.equals("table")) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
        {
            if (databaseName.equals("database") && tableName.equals("table") && parts.equals(ImmutableList.of("value"))) {
                return Optional.of(ImmutableList.of("value"));
            }

            return Optional.empty();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
        {
            if (databaseName.equals("database") && tableName.equals("table") && partitionNames.contains("value")) {
                return ImmutableMap.of("value", Optional.of(PARTITION));
            }

            return ImmutableMap.of();
        }

        @Override
        public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal principal)
        {
            if (databaseName.equals("database") && tableName.equals("table") && principal.getType() == USER && principal.getName().equals("user")) {
                return ImmutableSet.of(PRIVILEGE_INFO);
            }

            return ImmutableSet.of();
        }

        @Override
        public Set<String> listRoles()
        {
            return ImmutableSet.of("role");
        }

        @Override
        public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
        {
            return ImmutableSet.of(ROLE_GRANT);
        }
    }
}
