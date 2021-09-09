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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class BaseTestHiveInsertOverwrite
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA = "hive_insert_overwrite";

    private String bucketName;
    private HiveMinioDataLake dockerizedS3DataLake;

    private final String hiveHadoopImage;

    public BaseTestHiveInsertOverwrite(String hiveHadoopImage)
    {
        this.hiveHadoopImage = requireNonNull(hiveHadoopImage, "hiveHadoopImage is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-insert-overwrite-" + randomTableSuffix();
        this.dockerizedS3DataLake = closeAfterClass(
                new HiveMinioDataLake(bucketName, ImmutableMap.of(), hiveHadoopImage));
        this.dockerizedS3DataLake.start();
        return S3HiveQueryRunner.create(
                this.dockerizedS3DataLake.getHiveHadoop().getHiveMetastoreEndpoint(),
                this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint(),
                HiveMinioDataLake.ACCESS_KEY,
                HiveMinioDataLake.SECRET_KEY,
                ImmutableMap.<String, String>builder()
                        // This is required when using MinIO which requires path style access
                        .put("hive.s3.path-style-access", "true")
                        .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .build());
    }

    @BeforeClass
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA,
                bucketName));
    }

    @Test
    public void testInsertOverwriteNonPartitionedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(testTable));
        assertInsertFailure(
                testTable,
                "Overwriting unpartitioned table not supported when writing directly to target directory");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwriteNonPartitionedBucketedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        assertInsertFailure(
                testTable,
                "Overwriting unpartitioned table not supported when writing directly to target directory");
        computeActual(format("DROP TABLE %s", testTable));
    }

    @Test
    public void testInsertOverwritePartitionedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedTable()
    {
        String testTable = getTestTableName();
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(testTable);
    }

    @Test
    public void testInsertOverwritePartitionedAndBucketedExternalTable()
    {
        String testTable = getTestTableName();
        // Store table data in data lake bucket
        computeActual(getCreateTableStatement(
                testTable,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3"));
        copyTpchNationToTable(testTable);

        // Map this table as external table
        String externalTableName = testTable + "_ext";
        computeActual(getCreateTableStatement(
                externalTableName,
                "partitioned_by=ARRAY['regionkey']",
                "bucketed_by = ARRAY['nationkey']",
                "bucket_count = 3",
                format("external_location = 's3a://%s/%s/%s/'", this.bucketName, HIVE_TEST_SCHEMA, testTable)));
        copyTpchNationToTable(testTable);
        assertOverwritePartition(externalTableName);
    }

    protected void assertInsertFailure(String testTable, String expectedMessageRegExp)
    {
        assertQueryFails(
                format("INSERT INTO %s " +
                                "SELECT name, comment, nationkey, regionkey " +
                                "FROM tpch.tiny.nation",
                        testTable),
                expectedMessageRegExp);
    }

    protected void assertOverwritePartition(String testTable)
    {
        computeActual(format(
                "INSERT INTO %s VALUES " +
                        "('POLAND', 'Test Data', 25, 5), " +
                        "('CZECH', 'Test Data', 26, 5)",
                testTable));
        query(format("SELECT count(*) FROM %s WHERE regionkey = 5", testTable))
                .assertThat()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(2L)
                        .build());

        query(format("INSERT INTO %s values('POLAND', 'Overwrite', 25, 5)", testTable))
                .assertThat()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(1L)
                        .build());
        computeActual(format("DROP TABLE %s", testTable));
    }

    protected String getTestTableName()
    {
        return format("hive.%s.%s", HIVE_TEST_SCHEMA, "nation_" + randomTableSuffix());
    }

    protected String getCreateTableStatement(String tableName, String... propertiesEntries)
    {
        return getCreateTableStatement(tableName, Arrays.asList(propertiesEntries));
    }

    protected String getCreateTableStatement(String tableName, List<String> propertiesEntries)
    {
        return format(
                "CREATE TABLE %s (" +
                        "    name varchar(25), " +
                        "    comment varchar(152),  " +
                        "    nationkey bigint, " +
                        "    regionkey bigint) " +
                        (propertiesEntries.size() < 1 ? "" : propertiesEntries
                                .stream()
                                .collect(joining(",", "WITH (", ")"))),
                tableName);
    }

    protected void copyTpchNationToTable(String testTable)
    {
        computeActual(format("INSERT INTO " + testTable + " SELECT name, comment, nationkey, regionkey FROM tpch.tiny.nation"));
    }
}
