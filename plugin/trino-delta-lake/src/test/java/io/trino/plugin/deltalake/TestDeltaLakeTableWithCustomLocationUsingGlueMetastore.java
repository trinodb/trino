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

import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeTableWithCustomLocationUsingGlueMetastore
        extends BaseDeltaLakeTableWithCustomLocation
{
    private GlueHiveMetastore metastore;
    private String schema;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseDir = Files.createTempDirectory("warehouse-dir");
        closeAfterClass(() -> deleteRecursively(warehouseDir, ALLOW_INSECURE));
        metastore = createTestingGlueHiveMetastore(warehouseDir);
        schema = "test_tables_with_custom_location" + randomNameSuffix();
        return DeltaLakeQueryRunner.builder(schema)
                .addDeltaProperty("hive.metastore", "glue")
                .addDeltaProperty("hive.metastore.glue.region", requireNonNull(System.getenv("AWS_REGION"), "AWS_REGION is null"))
                .addDeltaProperty("hive.metastore.glue.default-warehouse-dir", warehouseDir.toUri().toString())
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        // Data is on the local disk and will be deleted by query runner cleanup
        metastore.dropDatabase(schema, false);
    }
}
