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
package io.trino.plugin.iceberg;

import io.trino.execution.DynamicFilterConfig;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestDynamicRowFiltering;
import io.trino.testing.QueryRunner;

import static com.google.common.base.Verify.verify;

public class TestIcebergDynamicRowFiltering
        extends AbstractTestDynamicRowFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");

        return IcebergQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorTableHandle)
    {
        return ((IcebergTableHandle) connectorTableHandle).getSchemaTableName();
    }
}
