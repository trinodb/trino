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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestDeltaLakeViewsGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_delta_lake_glue_views_" + randomTableSuffix();
    private static final String CATALOG_NAME = "test_delta_lake_glue_views";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DeltaLakeQueryRunner.builder(deltaLakeSession)
                .setCatalogName(CATALOG_NAME)
                .setDeltaProperties(ImmutableMap.of("hive.metastore", "glue"))
                .build();
        File schemaLocation = new File(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString());
        schemaLocation.deleteOnExit();
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + schemaLocation.getPath() + "')");
        return queryRunner;
    }

    @Test
    public void testCreateView()
    {
        String view = "test_glue_view_" + randomTableSuffix();
        try {
            assertUpdate(format("CREATE VIEW %s AS SELECT 1 AS val ", view), 1);
            assertQuery("SELECT val FROM " + view, "VALUES 1");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + view);
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertUpdate("DROP SCHEMA IF EXISTS " + SCHEMA);
    }
}
