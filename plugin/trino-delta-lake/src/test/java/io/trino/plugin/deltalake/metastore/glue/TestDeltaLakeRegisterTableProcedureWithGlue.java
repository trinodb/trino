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
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;

import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestDeltaLakeRegisterTableProcedureWithGlue
        extends BaseDeltaLakeRegisterTableProcedureTest
{
    private GlueHiveMetastore metastore;
    private String schema;
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        bucketName = "test-delta-lake-register-table-" + randomNameSuffix();
        floci.createBucket(bucketName);
        schema = "test_delta_lake_register_table" + randomNameSuffix();
        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(schema)
                .addDeltaProperty("hive.metastore", "glue")
                .addDeltaProperty("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .addDeltaProperty("fs.s3.enabled", "true")
                .addDeltaProperties(floci.s3AndGlueProperties())
                .addDeltaProperty("delta.unique-table-location", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .setSchemaLocation("s3://%s/%s".formatted(bucketName, schema))
                .build();
        metastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        return queryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        if (metastore != null) {
            metastore.dropDatabase(schema, false);
        }
    }

    @Override
    protected String nonExistingTableLocation()
    {
        return "s3://%s/non-existing-table".formatted(bucketName);
    }
}
