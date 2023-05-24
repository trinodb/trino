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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;

public class TestJdbcCachingConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createH2QueryRunner(REQUIRED_TPCH_TABLES, ImmutableMap.<String, String>builder()
                .putAll(TestingH2JdbcModule.createProperties())
                .put("metadata.cache-ttl", "10m")
                .put("metadata.cache-missing", "true")
                .put("case-insensitive-name-matching", "true")
                .buildOrThrow());
    }

    @Override
    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
