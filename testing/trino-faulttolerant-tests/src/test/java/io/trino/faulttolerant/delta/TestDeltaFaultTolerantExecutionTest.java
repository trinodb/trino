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

import io.trino.faulttolerant.BaseFaultTolerantExecutionTest;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestDeltaFaultTolerantExecutionTest
        extends BaseFaultTolerantExecutionTest
{
    private final String bucketName = "test-fte-preferred-write-partitioning-" + randomNameSuffix();

    public TestDeltaFaultTolerantExecutionTest()
    {
        super("partitioned_by");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        MinioStorage minioStorage = closeAfterClass(new MinioStorage(bucketName));
        minioStorage.start();

        return DeltaLakeQueryRunner.builder()
                .addExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setAdditionalSetup(instance -> {
                    instance.installPlugin(new FileSystemExchangePlugin());
                    instance.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .build();
    }
}
