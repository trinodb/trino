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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Sort temp files are deleted before the write finishes, so the test finds their names in file system spans, which concurrent queries invalidate
@Execution(SAME_THREAD)
final class TestIcebergFileNameProvider
        extends AbstractTestQueryFramework
{
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        IcebergFileNameProvider fileNameProvider = new IcebergFileNameProvider()
        {
            @Override
            public String newFileName(ConnectorSession session, FileContent content, FileFormat format)
            {
                return format.addExtension("custom-" + content.name().toLowerCase(ENGLISH) + "-" + randomUUID());
            }

            @Override
            public String newSortTempFilePrefix(ConnectorSession session)
            {
                return "custom-sort-" + randomUUID();
            }
        };
        return IcebergQueryRunner.builder()
                // Small enough that a sorted write of tpch.tiny.lineitem spills to temporary files
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .setAdditionalOverrideModule(binder -> newOptionalBinder(binder, IcebergFileNameProvider.class)
                        .setBinding()
                        .toInstance(fileNameProvider))
                .build();
    }

    @Test
    void testPositionDeleteFileName()
    {
        assertFileNamesAfterDelete(2, "custom-data-<uuid>.parquet", "custom-position_deletes-<uuid>.parquet");
    }

    @Test
    void testDeletionVectorFileName()
    {
        assertFileNamesAfterDelete(3, "custom-data-<uuid>.parquet", "custom-position_deletes-<uuid>.puffin");
    }

    @Test
    void testOptimizeFileName()
    {
        try (TestTable table = newTrinoTable("test_optimize_file_name", "(x integer) WITH (format = 'PARQUET')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2", 2);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3, 4", 2);
            Set<Object> filesBeforeOptimize = computeActual("SELECT file_path FROM \"" + table.getName() + "$files\"").getOnlyColumnAsSet();
            assertThat(filesBeforeOptimize).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize");

            assertThat(computeActual("SELECT file_path FROM \"" + table.getName() + "$files\"").getOnlyColumnAsSet())
                    .doesNotContainAnyElementsOf(filesBeforeOptimize);
            assertThat(fileNames(table)).containsExactly("custom-data-<uuid>.parquet");
        }
    }

    @Test
    void testSortTempFileName()
    {
        try (TestTable table = newTrinoTable("test_sort_temp_file_name", "WITH (sorted_by = ARRAY['comment'], format = 'PARQUET') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            assertUpdate("INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem", 60175);

            List<String> tempFileNames = getDistributedQueryRunner().getSpans().stream()
                    .filter(span -> span.getName().startsWith("OutputFile."))
                    .map(span -> Location.of(span.getAttributes().get(FILE_LOCATION)))
                    .filter(location -> location.parentDirectory().fileName().equals("trino-tmp-files"))
                    .map(Location::fileName)
                    .collect(toImmutableList());
            assertThat(tempFileNames).isNotEmpty().allMatch(name -> name.matches("custom-sort-" + UUID_PATTERN + "\\.\\d+"));
        }
    }

    private void assertFileNamesAfterDelete(int formatVersion, String dataFileName, String deleteFileName)
    {
        try (TestTable table = newTrinoTable("test_file_name", "(x integer) WITH (format_version = %s, format = 'PARQUET')".formatted(formatVersion), ImmutableList.of("1", "2", "3"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 2", 1);
            assertThat(fileNames(table)).containsExactlyInAnyOrder(dataFileName, deleteFileName);
        }
    }

    private Set<Object> fileNames(TestTable table)
    {
        String sql = "SELECT regexp_replace(regexp_extract(file_path, '[^/]+$'), '%s', '<uuid>') FROM \"%s$files\"".formatted(UUID_PATTERN, table.getName());
        return computeActual(sql).getOnlyColumnAsSet();
    }
}
