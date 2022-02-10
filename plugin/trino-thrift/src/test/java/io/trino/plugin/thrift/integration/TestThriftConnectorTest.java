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
package io.trino.plugin.thrift.integration;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import static io.trino.plugin.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.QueryAssertions.assertContains;

public class TestThriftConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_INSERT:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
                return false;

            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_COMMENT_ON_TABLE:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createThriftQueryRunner(3, false, ImmutableMap.of());
    }

    @Override
    @Test
    public void testShowSchemas()
    {
        MaterializedResult actualSchemas = computeActual("SHOW SCHEMAS").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("tiny")
                .row("sf1");
        assertContains(actualSchemas, resultBuilder.build());
    }
}
