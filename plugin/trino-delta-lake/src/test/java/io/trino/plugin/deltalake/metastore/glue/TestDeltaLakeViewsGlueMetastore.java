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

import io.trino.Session;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDeltaLakeViewsGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_delta_lake_glue_views_" + randomNameSuffix();
    private static final String CATALOG_NAME = "test_delta_lake_glue_views";
    private String dataDirectory;
    private HiveMetastore metastore;

    private HiveMetastore createTestMetastore(String dataDirectory)
    {
        return createTestingGlueHiveMetastore(dataDirectory);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("data_delta_lake_views").toString();
        metastore = createTestMetastore(dataDirectory);

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.of(new TestingDeltaLakeMetastoreModule(metastore)), EMPTY_MODULE));
        queryRunner.createCatalog(CATALOG_NAME, "delta_lake");

        queryRunner.execute("CREATE SCHEMA " + SCHEMA);
        return queryRunner;
    }

    @Test
    public void testCreateView()
    {
        String tableName = "test_glue_table_" + randomNameSuffix();
        String viewName = "test_glue_view_" + randomNameSuffix();
        try (TestTable table = new TestTable(getQueryRunner()::execute, tableName, "AS SELECT 'test' x");
                TestView view = new TestView(getQueryRunner()::execute, viewName, "SELECT * FROM " + table.getName())) {
            assertQuery(format("SELECT * FROM %s", view.getName()), "VALUES 'test'");
            assertQuery(format("SELECT table_type FROM information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", view.getName(), SCHEMA), "VALUES 'VIEW'");
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(Path.of(dataDirectory), ALLOW_INSECURE);
        }
    }
}
