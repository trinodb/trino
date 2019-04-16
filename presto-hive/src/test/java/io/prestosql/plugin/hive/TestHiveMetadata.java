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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveMetadata.createPredicate;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestHiveMetadata
{
    private static final HiveColumnHandle TEST_COLUMN_HANDLE = new HiveColumnHandle(
            "test",
            HiveType.HIVE_STRING,
            TypeSignature.parseTypeSignature("varchar"),
            0,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty());

    @Test(timeOut = 5000)
    public void testCreatePredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5_000; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))))));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateOnlyNullsPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR))));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateMixedPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))))));
        }

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "null",
                ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR))));

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testGetTablePrincipalPrivileges()
    {
        SemiTransactionalHiveMetastore metastore = mock(SemiTransactionalHiveMetastore.class);
        doReturn(ImmutableSet.of(
                hivePrivilegeInfo(PrincipalType.USER, "user001"),
                hivePrivilegeInfo(PrincipalType.USER, "user002"),
                hivePrivilegeInfo(PrincipalType.ROLE, "role001")
        )).when(metastore).listTablePrivileges(anyString(), anyString(), eq(null));

        HdfsEnvironment hdfsEnvironment = mock(HdfsEnvironment.class);
        HivePartitionManager partitionManager = mock(HivePartitionManager.class);
        DateTimeZone timeZone = mock(DateTimeZone.class);
        boolean allowCorruptWritesForTesting = true;
        boolean writesToNonManagedTablesEnabled = true;
        boolean createsOfNonManagedTablesEnabled = true;
        TypeManager typeManager = mock(TypeManager.class);
        LocationService locationService = mock(LocationService.class);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = mock(JsonCodec.class);
        TypeTranslator typeTranslator = mock(TypeTranslator.class);
        String prestoVersion = "version001";
        HiveStatisticsProvider hiveStatisticsProvider = mock(HiveStatisticsProvider.class);
        int maxPartitions = 100;

        PrincipalPrivileges principalPrivileges = new HiveMetadata(
                metastore, hdfsEnvironment, partitionManager, timeZone, allowCorruptWritesForTesting, writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled, typeManager, locationService, partitionUpdateCodec, typeTranslator,
                prestoVersion, hiveStatisticsProvider, maxPartitions
        ).getTablePrincipalPrivileges("db001", "tbl001");

        assertNotNull(principalPrivileges);
        assertEquals(
                2, principalPrivileges.getUserPrivileges().size());
        assertEquals(
                1, principalPrivileges.getRolePrivileges().size());
    }

    private HivePrivilegeInfo hivePrivilegeInfo(PrincipalType type, String key)
    {
        HivePrivilegeInfo privilege1 = mock(HivePrivilegeInfo.class);
        HivePrincipal principal = mock(HivePrincipal.class);
        doReturn(type).when(principal).getType();
        doReturn(key).when(principal).getName();
        doReturn(principal).when(privilege1).getGrantee();

        return privilege1;
    }
}
