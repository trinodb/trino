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
package io.trino.faulttolerant.delta;

import com.google.common.collect.ImmutableMap;
import io.trino.faulttolerant.BaseFaultTolerantExecutionTest;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaFaultTolerantExecutionTest
        extends BaseFaultTolerantExecutionTest
{
    private static final String SCHEMA = "fte_preferred_write_partitioning";
    private static final String BUCKET_NAME = "test-fte-preferred-write-partitioning-" + randomNameSuffix();

    public TestDeltaFaultTolerantExecutionTest()
    {
        super("partitioned_by");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(BUCKET_NAME));
        hiveMinioDataLake.start();
        MinioStorage minioStorage = closeAfterClass(new MinioStorage(BUCKET_NAME));
        minioStorage.start();

        DistributedQueryRunner runner = createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                FaultTolerantExecutionConnectorTestHelper.getExtraProperties(),
                ImmutableMap.of(),
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop(),
                instance -> {
                    instance.installPlugin(new FileSystemExchangePlugin());
                    instance.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                });
        runner.execute(format("CREATE SCHEMA %s WITH (location = 's3://%s/%s')", SCHEMA, BUCKET_NAME, SCHEMA));
        return runner;
    }

    @Override
    public void testExecutePreferredWritePartitioningSkewMitigation()
    {
        assertThatThrownBy(super::testExecutePreferredWritePartitioningSkewMitigation)
                .hasMessage("optimize is expected to generate more than a single file per partition");
    }
}
