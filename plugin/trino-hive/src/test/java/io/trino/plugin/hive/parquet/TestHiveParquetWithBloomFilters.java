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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveParquetWithBloomFilters
        extends AbstractTestQueryFramework
{
    private static final String COLUMN_NAME = "dataColumn";
    // containing extreme values, so the row group cannot be eliminated by the column chunk's min/max statistics
    private static final List<Integer> TEST_VALUES = Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE, 1, 3, 7, 10, 15);
    private static final int MISSING_VALUE = 0;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder().build();
    }

    @Test
    public void verifyBloomFilterEnabled()
    {
        assertThat(query(format("SHOW SESSION LIKE '%s.parquet_use_bloom_filter'", getSession().getCatalog().orElseThrow())))
                .skippingTypesCheck()
                .matches(result -> result.getRowCount() == 1)
                .matches(result -> {
                    String value = (String) result.getMaterializedRows().get(0).getField(1);
                    return value.equals("true");
                });
    }

    @Test
    public void testBloomFilterRowGroupPruning()
            throws Exception
    {
        File tmpDir = Files.createTempDirectory("testBloomFilterRowGroupPruning").toFile();
        try {
            File parquetFile = new File(tmpDir, randomNameSuffix());

            String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
            createParquetBloomFilterSource(parquetFile, COLUMN_NAME, TEST_VALUES);
            assertUpdate(
                    format(
                            "CREATE TABLE %s (%s INT) WITH (format = 'PARQUET', external_location = '%s')",
                            tableName,
                            COLUMN_NAME,
                            tmpDir.getAbsolutePath()));

            // When reading bloom filter is enabled, row groups are pruned when searching for a missing value
            assertQueryStats(
                    getSession(),
                    "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + MISSING_VALUE,
                    queryStats -> {
                        assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                        assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                    },
                    results -> assertThat(results.getRowCount()).isEqualTo(0));

            // When reading bloom filter is enabled, row groups are not pruned when searching for a value present in the file
            assertQueryStats(
                    getSession(),
                    "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + TEST_VALUES.get(0),
                    queryStats -> {
                        assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                        assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                    },
                    results -> assertThat(results.getRowCount()).isEqualTo(1));

            // When reading bloom filter is disabled, row groups are not pruned when searching for a missing value
            assertQueryStats(
                    bloomFiltersDisabled(getSession()),
                    "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + MISSING_VALUE,
                    queryStats -> {
                        assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                        assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                    },
                    results -> assertThat(results.getRowCount()).isEqualTo(0));
        }
        finally {
            deleteRecursively(tmpDir.toPath(), ALLOW_INSECURE);
        }
    }

    private static Session bloomFiltersDisabled(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_bloom_filter", "false")
                .build();
    }

    private static void createParquetBloomFilterSource(File tempFile, String columnName, List<Integer> testValues)
            throws Exception
    {
        List<ObjectInspector> objectInspectors = singletonList(javaIntObjectInspector);
        List<String> columnNames = ImmutableList.of(columnName);

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
        jobConf.setBoolean(BLOOM_FILTER_ENABLED, true);

        ParquetTester.writeParquetColumn(
                jobConf,
                tempFile,
                SNAPPY,
                ParquetTester.createTableProperties(columnNames, objectInspectors),
                getStandardStructObjectInspector(columnNames, objectInspectors),
                new Iterator<?>[] {testValues.iterator()},
                Optional.empty(),
                false,
                DateTimeZone.getDefault());
    }
}
