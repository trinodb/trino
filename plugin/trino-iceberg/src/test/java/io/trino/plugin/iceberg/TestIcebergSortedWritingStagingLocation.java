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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSortedWritingStagingLocation
        extends AbstractTestQueryFramework
{
    private final Location stagingRoot = Location.of("local:///trino-sorted-staging-" + randomUUID());
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.LINE_ITEM))
                .addIcebergProperty("iceberg.sorted-writing-enabled", "true")
                .addIcebergProperty("iceberg.sorted-writing.staging-location", stagingRoot.appendPath("${USER}").toString())
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .build();
    }

    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        fileSystem.deleteDirectory(stagingRoot);
    }

    @Test
    public void testSortedWritingStagesUnderTableAndQuery()
            throws IOException
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_sorted_staging_location",
                "WITH (sorted_by = ARRAY['comment'], format = 'PARQUET') AS TABLE tpch.tiny.lineitem WITH NO DATA")) {
            MaterializedResultWithPlan insert = getDistributedQueryRunner().executeWithPlan(getSession(), "INSERT INTO " + table.getName() + " TABLE tpch.tiny.lineitem");
            assertThat(insert.result().getUpdateCount()).hasValue(60175);

            for (Object filePath : computeActual("SELECT file_path FROM \"" + table.getName() + "$files\"").getOnlyColumnAsSet()) {
                assertThat(checkParquetFileSorting(fileSystem.newInputFile(Location.of((String) filePath)), "comment")).isTrue();
            }

            Location queryDirectory = stagingRoot
                    .appendPath(getSession().getUser())
                    .appendPath(getSession().getSchema().orElseThrow())
                    .appendPath(table.getName())
                    .appendPath(insert.queryId().toString());
            assertThat(fileSystem.directoryExists(queryDirectory)).hasValue(true);
            assertThat(fileSystem.listFiles(queryDirectory).hasNext()).isFalse();
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM lineitem");
        }
    }
}
