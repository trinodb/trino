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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;

public class TestCassandraLatestConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new CassandraServer("3.11.9"));
        return createCassandraQueryRunner(server, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_DELETE:
                return true;

            case SUPPORTS_ROW_LEVEL_DELETE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
