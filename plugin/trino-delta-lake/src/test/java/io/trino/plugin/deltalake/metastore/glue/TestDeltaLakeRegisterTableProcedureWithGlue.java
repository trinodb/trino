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

import io.trino.plugin.deltalake.BaseDeltaLakeRegisterTableProcedureTest;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestDeltaLakeRegisterTableProcedureWithGlue
        extends BaseDeltaLakeRegisterTableProcedureTest
{
    private GlueHiveMetastore metastore;
    private String schema;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseDir = Files.createTempDirectory("warehouse-dir");
        closeAfterClass(() -> deleteRecursively(warehouseDir, ALLOW_INSECURE));
        metastore = createTestingGlueHiveMetastore(warehouseDir, this::closeAfterClass);
        schema = "test_delta_lake_register_table" + randomNameSuffix();
        return DeltaLakeQueryRunner.builder(schema)
                .addDeltaProperty("hive.metastore", "glue")
                .addDeltaProperty("hive.metastore.glue.region", requireEnv("AWS_REGION"))
                .addDeltaProperty("hive.metastore.glue.default-warehouse-dir", warehouseDir.toUri().toString())
                .addDeltaProperty("delta.unique-table-location", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        // Data is on the local disk and will be deleted by query runner cleanup
        metastore.dropDatabase(schema, false);
        metastore.shutdown();
    }
}
