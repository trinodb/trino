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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.io.File;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDeltaLakeTableWithCustomLocationUsingGlueMetastore
        extends BaseDeltaLakeTableWithCustomLocation
{
    private static final Logger LOG = Logger.get(TestDeltaLakeTableWithCustomLocationUsingGlueMetastore.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        this.metastoreDir = new File(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString());
        this.metastoreDir.deleteOnExit();

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                CATALOG_NAME,
                CONNECTOR_NAME,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.region", "us-east-2")
                        .put("hive.metastore.glue.default-warehouse-dir", metastoreDir.toURI().toString())
                        .buildOrThrow());

        metastore = createTestingGlueHiveMetastore(metastoreDir.toPath());

        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + metastoreDir.toURI() + "')");
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        try {
            if (metastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                metastore.dropDatabase(SCHEMA, false);
                deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", SCHEMA);
        }
    }
}
