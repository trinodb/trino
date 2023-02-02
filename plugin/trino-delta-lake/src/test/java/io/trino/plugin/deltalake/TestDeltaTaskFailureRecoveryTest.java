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
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestDeltaTaskFailureRecoveryTest
        extends BaseDeltaFailureRecoveryTest
{
    private static final String SCHEMA = "task_failure_recovery";
    private final String bucketName = "test-delta-lake-task-failure-recovery-" + randomNameSuffix();

    protected TestDeltaTaskFailureRecoveryTest()
    {
        super(RetryPolicy.TASK);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        MinioStorage minioStorage = closeAfterClass(new MinioStorage("test-exchange-spooling-" + randomNameSuffix()));
        minioStorage.start();

        DistributedQueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                configProperties,
                coordinatorProperties,
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop(),
                runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                });
        queryRunner.execute(format("CREATE SCHEMA %s WITH (location = 's3://%s/%s')", SCHEMA, bucketName, SCHEMA));
        requiredTpchTables.forEach(table -> queryRunner.execute(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.%1$s", table.getTableName())));

        return queryRunner;
    }
}
