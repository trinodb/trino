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
package io.trino.faulttolerant.iceberg;

import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.TestIcebergParquetConnectorTest;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.FaultTolerantExecutionConnectorTestHelper.getExtraProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergParquetFaultTolerantExecutionConnectorTest
        extends TestIcebergParquetConnectorTest
{
    private MinioStorage minioStorage;

    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomNameSuffix());
        minioStorage.start();

        IcebergQueryRunner.Builder builder = super.createQueryRunnerBuilder();
        getExtraProperties().forEach(builder::addExtraProperty);
        builder.setAdditionalSetup(runner -> {
            runner.installPlugin(new FileSystemExchangePlugin());
            runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
        });
        return builder;
    }

    @Override
    public void testSplitPruningForFilterOnPartitionColumn()
    {
        // TODO: figure out why
        assertThatThrownBy(super::testSplitPruningForFilterOnPartitionColumn)
                .hasMessageContaining("Couldn't find operator summary, probably due to query statistic collection error");
        throw new SkipException("fails currently on FTE");
    }

    @Override
    public void testStatsBasedRepartitionDataOnCtas()
    {
        // TODO: figure out why
        throw new SkipException("We always get 3 partitions with FTE");
    }

    @Override
    public void testStatsBasedRepartitionDataOnInsert()
    {
        // TODO: figure out why
        throw new SkipException("We always get 3 partitions with FTE");
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkParquetFileSorting(path, sortColumnName);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
