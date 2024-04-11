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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.io.File;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeTableWithCustomLocationUsingGlueMetastore
        extends BaseDeltaLakeTableWithCustomLocation
{
    private static final Logger LOG = Logger.get(TestDeltaLakeTableWithCustomLocationUsingGlueMetastore.class);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(SCHEMA)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        this.metastoreDir = new File(queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data").toString());
        this.metastoreDir.deleteOnExit();

        queryRunner.installPlugin(new DeltaLakePlugin());
        queryRunner.createCatalog(
                DELTA_CATALOG,
                CONNECTOR_NAME,
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "glue")
                        .put("hive.metastore.glue.region", requireNonNull(System.getenv("AWS_REGION"), "AWS_REGION is null"))
                        .put("hive.metastore.glue.default-warehouse-dir", metastoreDir.toURI().toString())
                        .buildOrThrow());

        metastore = createTestingGlueHiveMetastore(metastoreDir.toPath());

        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = '" + metastoreDir.toURI() + "')");
        return queryRunner;
    }

    @AfterAll
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
