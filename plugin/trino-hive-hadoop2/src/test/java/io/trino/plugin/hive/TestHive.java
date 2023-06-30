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
import com.google.common.net.HostAndPort;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.net.NetUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

// staging directory is shared mutable state
@Test(singleThreaded = true)
public class TestHive
        extends AbstractTestHive
{
    @Parameters({"test.metastore", "test.database"})
    @BeforeClass
    public void initialize(String metastore, String database)
    {
        String hadoopMasterIp = System.getProperty("hadoop-master-ip");
        if (hadoopMasterIp != null) {
            // Even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
            // (e.g: in: NameNodeProxies.createProxy)
            // This adds a static resolution for hadoop-master to docker container internal ip
            NetUtils.addStaticResolution("hadoop-master", hadoopMasterIp);
        }

        setup(HostAndPort.fromString(metastore), database);
    }

    @Test
    public void forceTestNgToRespectSingleThreaded()
    {
        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
    }

    @Override
    public void testHideDeltaLakeTables()
    {
        assertThatThrownBy(super::testHideDeltaLakeTables)
                .hasMessageMatching("(?s)\n" +
                        "Expecting\n" +
                        "  \\[.*\\b(\\w+.tmp_trino_test_trino_delta_lake_table_\\w+)\\b.*]\n" +
                        "not to contain\n" +
                        "  \\[\\1]\n" +
                        "but found.*");

        throw new SkipException("not supported");
    }

    @Test
    public void testHiveViewTranslationError()
    {
        try (Transaction transaction = newTransaction()) {
            assertThatThrownBy(() -> transaction.getMetadata().getView(newSession(), view))
                    .isInstanceOf(HiveViewNotSupportedException.class)
                    .hasMessageContaining("Hive views are not supported");

            // TODO: combine this with tests for successful translation (currently in TestHiveViews product test)
        }
    }

    @Override
    public void testUpdateBasicPartitionStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_basic_partition_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
            // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
            // used to ingest data into partitioned hive tables.
            testUpdatePartitionStatistics(
                    tableName,
                    PartitionStatistics.empty(),
                    ImmutableList.of(BASIC_STATISTICS_1, BASIC_STATISTICS_2),
                    ImmutableList.of(BASIC_STATISTICS_2, BASIC_STATISTICS_1));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Override
    public void testUpdatePartitionColumnStatistics()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
            // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
            // used to ingest data into partitioned hive tables.
            testUpdatePartitionStatistics(
                    tableName,
                    PartitionStatistics.empty(),
                    ImmutableList.of(STATISTICS_1_1, STATISTICS_1_2, STATISTICS_2),
                    ImmutableList.of(STATISTICS_1_2, STATISTICS_1_1, STATISTICS_2));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Override
    public void testUpdatePartitionColumnStatisticsEmptyOptionalFields()
            throws Exception
    {
        SchemaTableName tableName = temporaryTable("update_partition_column_statistics");
        try {
            createDummyPartitionedTable(tableName, STATISTICS_PARTITIONED_TABLE_COLUMNS);
            // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
            // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
            // used to ingest data into partitioned hive tables.
            testUpdatePartitionStatistics(
                    tableName,
                    PartitionStatistics.empty(),
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS),
                    ImmutableList.of(STATISTICS_EMPTY_OPTIONAL_FIELDS));
        }
        finally {
            dropTable(tableName);
        }
    }

    @Override
    public void testStorePartitionWithStatistics()
            throws Exception
    {
        // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
        // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
        // used to ingest data into partitioned hive tables.
        testStorePartitionWithStatistics(STATISTICS_PARTITIONED_TABLE_COLUMNS, STATISTICS_1, STATISTICS_2, STATISTICS_1_1, PartitionStatistics.empty());
    }
}
