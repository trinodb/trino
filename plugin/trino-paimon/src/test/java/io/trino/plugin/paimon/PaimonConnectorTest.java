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
package io.trino.plugin.paimon;

import io.trino.plugin.paimon.testing.TpchPaimonTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The test of TrinoDistributedQuery.
 */
final class PaimonConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PaimonQueryRunner.builder()
                .setDataLoader(new TpchPaimonTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageFindingMatch("(Fetch splits failed!|Failed to get table metadata!)");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_INSERT,
                 SUPPORTS_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_MERGE,
                 SUPPORTS_ARRAY,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
