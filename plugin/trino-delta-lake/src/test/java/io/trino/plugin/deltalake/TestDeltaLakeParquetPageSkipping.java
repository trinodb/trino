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
package io.trino.plugin.deltalake;

import com.google.common.io.Resources;
import io.trino.plugin.hive.BaseTestParquetPageSkipping;
import io.trino.spi.metrics.Count;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeParquetPageSkipping
        extends BaseTestParquetPageSkipping
{
    private static final String LINEITEM_COLUMNS = "(suppkey bigint, extendedprice decimal(12, 2), shipmode varchar, comment varchar)";
    // Rows sorted by suppkey, so this range lies in pages in the middle of the file
    private static final String TARGET_ROWS_PREDICATE = "suppkey BETWEEN 25 AND 35";

    private Path catalogDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalogDir = Files.createTempDirectory("delta-page-skipping");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("parquet.use-column-index", "true")
                .addDeltaProperty("parquet.max-buffer-size", "1MB")
                .build();
    }

    @Override
    protected String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName)
            throws IOException
    {
        return createTableWithDataFile(tableNamePrefix, columnsDefinition, resourceFileName, "");
    }

    @Override
    protected String timestampMillisType()
    {
        return "timestamp(3) with time zone";
    }

    @Test
    public void testDeleteWithPageSkipping()
            throws Exception
    {
        testDeleteWithPageSkipping(false);
        testDeleteWithPageSkipping(true);
    }

    private void testDeleteWithPageSkipping(boolean deletionVectorsEnabled)
            throws Exception
    {
        String tableName = createLineitemTable(deletionVectorsEnabled);
        long totalRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName);
        long targetRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE " + TARGET_ROWS_PREDICATE);
        assertThat(targetRows).isGreaterThan(0);

        assertUpdateWithPageSkipping("DELETE FROM " + tableName + " WHERE " + TARGET_ROWS_PREDICATE, targetRows);

        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName)).isEqualTo(totalRows - targetRows);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE " + TARGET_ROWS_PREDICATE)).isEqualTo(0);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE suppkey > 50")).isGreaterThan(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUpdateWithPageSkipping()
            throws Exception
    {
        testUpdateWithPageSkipping(false);
        testUpdateWithPageSkipping(true);
    }

    private void testUpdateWithPageSkipping(boolean deletionVectorsEnabled)
            throws Exception
    {
        String tableName = createLineitemTable(deletionVectorsEnabled);
        long totalRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName);
        long targetRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE " + TARGET_ROWS_PREDICATE);
        assertThat(targetRows).isGreaterThan(0);

        assertUpdateWithPageSkipping("UPDATE " + tableName + " SET comment = 'updated' WHERE " + TARGET_ROWS_PREDICATE, targetRows);

        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName)).isEqualTo(totalRows);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE comment = 'updated'")).isEqualTo(targetRows);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE comment = 'updated' AND NOT (" + TARGET_ROWS_PREDICATE + ")")).isEqualTo(0);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE comment <> 'updated' AND " + TARGET_ROWS_PREDICATE)).isEqualTo(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeWithPageSkipping()
            throws Exception
    {
        testMergeWithPageSkipping(false);
        testMergeWithPageSkipping(true);
    }

    private void testMergeWithPageSkipping(boolean deletionVectorsEnabled)
            throws Exception
    {
        String tableName = createLineitemTable(deletionVectorsEnabled);
        long totalRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName);
        long targetRows = assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE " + TARGET_ROWS_PREDICATE);
        assertThat(targetRows).isGreaterThan(0);

        assertUpdateWithPageSkipping(
                format(
                        "MERGE INTO %s target USING (SELECT * FROM UNNEST(sequence(25, 35))) source(suppkey) " +
                                "ON target.suppkey = source.suppkey AND target.%s " +
                                "WHEN MATCHED THEN UPDATE SET comment = 'merged'",
                        tableName,
                        TARGET_ROWS_PREDICATE),
                targetRows);

        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName)).isEqualTo(totalRows);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE comment = 'merged'")).isEqualTo(targetRows);
        assertThat(assertColumnIndexResults("SELECT suppkey FROM " + tableName + " WHERE comment = 'merged' AND NOT (" + TARGET_ROWS_PREDICATE + ")")).isEqualTo(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    private String createLineitemTable(boolean deletionVectorsEnabled)
            throws IOException
    {
        return createTableWithDataFile(
                "test_lineitem",
                LINEITEM_COLUMNS,
                "parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet",
                ", deletion_vectors_enabled = " + deletionVectorsEnabled);
    }

    private String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName, String additionalTableProperties)
            throws IOException
    {
        String tableName = tableName(tableNamePrefix);
        Path tableLocation = catalogDir.resolve(tableName);
        assertUpdate(format("CREATE TABLE %s %s WITH (location = '%s'%s)", tableName, columnsDefinition, tableLocation.toUri(), additionalTableProperties));

        Path dataFile = tableLocation.resolve("data.parquet");
        try (OutputStream output = Files.newOutputStream(dataFile)) {
            Resources.copy(Resources.getResource(resourceFileName), output);
        }
        String addAction = format(
                "{\"add\":{\"path\":\"data.parquet\",\"partitionValues\":{},\"size\":%d,\"modificationTime\":%d,\"dataChange\":true}}",
                Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis());
        Files.writeString(tableLocation.resolve("_delta_log").resolve("00000000000000000001.json"), addAction + "\n");
        return tableName;
    }

    private void assertUpdateWithPageSkipping(@Language("SQL") String sql, long expectedUpdateCount)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        assertThat(result.result().getUpdateCount()).hasValue(expectedUpdateCount);
        long rowsFilteredByColumnIndex = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .map(summary -> summary.getConnectorMetrics().getMetrics().get(COLUMN_INDEX_ROWS_FILTERED))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
        assertThat(rowsFilteredByColumnIndex).isGreaterThan(0);
    }
}
