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
package io.trino.plugin.hive;

import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveFaultTolerantExecutionConnectorTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BaseHiveConnectorTest.createHiveQueryRunner(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
    }

    @Override
    public void testGroupedExecution()
    {
        // grouped execution is not supported (and not needed) with batch execution enabled
    }

    @Override
    public void testScaleWriters()
    {
        testWithAllStorageFormats(this::testSingleWriter);
    }

    @Override
    public void testTargetMaxFileSize()
    {
        testTargetMaxFileSize(1);
    }

    @Override
    public void testTargetMaxFileSizePartitioned()
    {
        testTargetMaxFileSizePartitioned(1);
    }

    @Override
    public void testOptimize()
    {
        assertThatThrownBy(super::testOptimize)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithWriterScaling()
    {
        assertThatThrownBy(super::testOptimizeWithWriterScaling)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithPartitioning()
    {
        assertThatThrownBy(super::testOptimizeWithPartitioning)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeWithBucketing()
    {
        assertThatThrownBy(super::testOptimizeWithBucketing)
                .hasMessageContaining("OPTIMIZE procedure is not supported with query retries enabled");
    }

    @Override
    public void testOptimizeHiveInformationSchema()
    {
        assertThatThrownBy(super::testOptimizeHiveInformationSchema)
                .hasMessageContaining("This connector does not support query retries");
    }

    @Override
    public void testOptimizeHiveSystemTable()
    {
        assertThatThrownBy(super::testOptimizeHiveSystemTable)
                .hasMessageContaining("This connector does not support query retries");
    }
}
