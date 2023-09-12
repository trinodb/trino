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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

// The test requires AWS credentials be provided via one of the ways used by the DefaultAWSCredentialsProviderChain.
public class TestS3SelectQueries
        extends AbstractTestQueryFramework
{
    private final String bucket;
    private final String bucketEndpoint;

    @Parameters({"s3.bucket", "s3.bucket-endpoint"})
    public TestS3SelectQueries(String bucket, String bucketEndpoint)
    {
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.bucketEndpoint = requireNonNull(bucketEndpoint, "bucketEndpoint is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> hiveProperties = ImmutableMap.builder();
        hiveProperties.put("hive.s3.endpoint", bucketEndpoint);
        hiveProperties.put("hive.non-managed-table-writes-enabled", "true");
        hiveProperties.put("hive.s3select-pushdown.experimental-textfile-pushdown-enabled", "true");
        return HiveQueryRunner.builder()
                .setHiveProperties(hiveProperties.buildOrThrow())
                .setInitialTables(ImmutableList.of())
                .setMetastore(queryRunner -> {
                    File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
                    return new FileHiveMetastore(
                            new NodeVersion("testversion"),
                            HDFS_FILE_SYSTEM_FACTORY,
                            new HiveMetastoreConfig().isHideDeltaLakeTables(),
                            new FileHiveMetastoreConfig()
                                    .setCatalogDirectory(baseDir.toURI().toString())
                                    .setMetastoreUser("test")
                                    .setDisableLocationChecks(true));
                })
                .build();
    }

    @Test(dataProvider = "s3SelectFileFormats")
    public void testS3SelectPushdown(String tableProperties)
    {
        Session usingAppendInserts = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "APPEND")
                .build();
        List<String> values = ImmutableList.of(
                "1, true, 11, 111, 1111, 11111, 'one', DATE '2020-01-01'",
                "2, true, 22, 222, 2222, 22222, 'two', DATE '2020-02-02'",
                "3, NULL, NULL, NULL, NULL, NULL, NULL, NULL",
                "4, false, 44, 444, 4444, 44444, '', DATE '2020-04-04'");
        try (TestTable table = new TestTable(
                sql -> getQueryRunner().execute(usingAppendInserts, sql),
                "hive.%s.test_s3_select_pushdown".formatted(HiveQueryRunner.TPCH_SCHEMA),
                "(id INT, bool_t BOOLEAN, tiny_t TINYINT, small_t SMALLINT, int_t INT, big_t BIGINT, string_t VARCHAR, date_t DATE) " +
                        "WITH (external_location = 's3://" + bucket + "/test_s3_select_pushdown/test_table_" + randomNameSuffix() + "', " + tableProperties + ")", values)) {
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE bool_t = true", "VALUES 1, 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE bool_t = false", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE bool_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE bool_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t = 22", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t != 22", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t > 22", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t >= 22", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t = 22 OR tiny_t = 44", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t IS NULL OR tiny_t >= 22", "VALUES 2, 3, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE tiny_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t = 222", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t != 222", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t > 222", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t >= 222", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t = 222 OR small_t = 444", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t IS NULL OR small_t >= 222", "VALUES 2, 3, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE small_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t = 2222", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t != 2222", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t > 2222", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t >= 2222", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t = 2222 OR int_t = 4444", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t IS NULL OR int_t >= 2222", "VALUES 2, 3, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE int_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t = 22222", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t != 22222", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t > 22222", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t >= 22222", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t = 22222 OR big_t = 44444", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t IS NULL OR big_t >= 22222", "VALUES 2, 3, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE big_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t = 'two'", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t != 'two'", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t < 'two'", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t <= 'two'", "VALUES 1, 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t = 'two' OR string_t = ''", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t IS NULL OR string_t >= 'two'", "VALUES 2, 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE string_t IS NOT NULL", "VALUES 1, 2, 4");

            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t = DATE '2020-02-02'", "VALUES 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t != DATE '2020-02-02'", "VALUES 1, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t > DATE '2020-02-02'", "VALUES 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t <= DATE '2020-02-02'", "VALUES 1, 2");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t = DATE '2020-02-02' OR date_t = DATE '2020-04-04'", "VALUES 2, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t IS NULL OR date_t >= DATE '2020-02-02'", "VALUES 2, 3, 4");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t IS NULL", "VALUES 3");
            assertS3SelectQuery("SELECT id FROM " + table.getName() + " WHERE date_t IS NOT NULL", "VALUES 1, 2, 4");
        }
    }

    private void assertS3SelectQuery(@Language("SQL") String query, @Language("SQL") String expectedValues)
    {
        Session withS3SelectPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "s3_select_pushdown_enabled", "true")
                .setCatalogSessionProperty("hive", "json_native_reader_enabled", "false")
                .setCatalogSessionProperty("hive", "text_file_native_reader_enabled", "false")
                .build();

        MaterializedResult expectedResult = computeActual(expectedValues);
        assertQueryStats(
                withS3SelectPushdown,
                query,
                statsWithPushdown -> {
                    long inputPositionsWithPushdown = statsWithPushdown.getPhysicalInputPositions();
                    assertQueryStats(
                            getSession(),
                            query,
                            statsWithoutPushdown -> assertThat(statsWithoutPushdown.getPhysicalInputPositions()).isGreaterThan(inputPositionsWithPushdown),
                            results -> assertEquals(results.getOnlyColumnAsSet(), expectedResult.getOnlyColumnAsSet()));
                },
                results -> assertEquals(results.getOnlyColumnAsSet(), expectedResult.getOnlyColumnAsSet()));
    }

    @DataProvider
    public static Object[][] s3SelectFileFormats()
    {
        return new Object[][] {
                {"format = 'JSON'"},
                {"format = 'TEXTFILE', textfile_field_separator=',', textfile_field_separator_escape='|', null_format='~'"}
        };
    }
}
