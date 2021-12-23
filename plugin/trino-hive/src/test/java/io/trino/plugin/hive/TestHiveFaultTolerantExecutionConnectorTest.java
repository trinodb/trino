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

import io.trino.testing.BaseFaultTolerantExecutionConnectorTest;

public class TestHiveFaultTolerantExecutionConnectorTest
        extends AbstractHiveConnectorTest
{
    public TestHiveFaultTolerantExecutionConnectorTest()
    {
        super(BaseFaultTolerantExecutionConnectorTest.getExtraProperties(), BaseFaultTolerantExecutionConnectorTest.getExchangeManagerPropertiesFile());
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
}
