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
package io.trino.plugin.deltalake.metastore.glue;

import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeViewsGlueMetastore
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private String schema;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseDir = Files.createTempDirectory("warehouse-dir");
        closeAfterClass(() -> deleteRecursively(warehouseDir, ALLOW_INSECURE));
        metastore = createTestingGlueHiveMetastore(warehouseDir);
        schema = "test_delta_lake_glue_views_" + randomNameSuffix();
        return DeltaLakeQueryRunner.builder(schema)
                .addDeltaProperty("hive.metastore", "glue")
                .addDeltaProperty("hive.metastore.glue.region", requireNonNull(System.getenv("AWS_REGION"), "AWS_REGION is null"))
                .addDeltaProperty("hive.metastore.glue.default-warehouse-dir", warehouseDir.toUri().toString())
                .build();
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        metastore.dropDatabase(schema, false);
    }

    @Test
    public void testCreateView()
    {
        String tableName = "test_glue_table_" + randomNameSuffix();
        String viewName = "test_glue_view_" + randomNameSuffix();
        try (TestTable table = new TestTable(getQueryRunner()::execute, tableName, "AS SELECT 'test' x");
                TestView view = new TestView(getQueryRunner()::execute, viewName, "SELECT * FROM " + table.getName())) {
            assertQuery(format("SELECT * FROM %s", view.getName()), "VALUES 'test'");
            assertQuery(format("SELECT table_type FROM information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", view.getName(), schema), "VALUES 'VIEW'");
            // Ensure all relations are being listed
            assertQuery(format("SELECT table_type FROM information_schema.tables WHERE table_name LIKE '%%%s' AND table_schema='%s'", view.getName(), schema), "VALUES 'VIEW'");
        }
    }
}
