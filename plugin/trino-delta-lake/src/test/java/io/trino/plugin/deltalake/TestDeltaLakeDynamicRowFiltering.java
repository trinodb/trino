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

import io.trino.execution.DynamicFilterConfig;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestDynamicRowFiltering;
import io.trino.testing.QueryRunner;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestDeltaLakeDynamicRowFiltering
        extends AbstractTestDynamicRowFiltering
{
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");

        String bucketName = "delta-test-dynamic-row-filtering-" + randomNameSuffix();
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();

        QueryRunner queryRunner = DeltaLakeQueryRunner.builder()
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();

        queryRunner.execute(format("CREATE SCHEMA IF NOT EXISTS %s.tpch", DELTA_CATALOG));

        REQUIRED_TPCH_TABLES.forEach(table -> {
            String tableName = table.getTableName();
            hiveMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/databricks73/" + tableName, tableName);
            queryRunner.execute(format("CALL %1$s.system.register_table('%2$s', '%3$s', 's3://%4$s/%3$s')",
                    DELTA_CATALOG,
                    "tpch",
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorHandle)
    {
        return ((DeltaLakeTableHandle) connectorHandle).getSchemaTableName();
    }
}
