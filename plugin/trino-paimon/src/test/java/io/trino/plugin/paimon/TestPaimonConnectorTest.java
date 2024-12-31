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

import io.trino.plugin.paimon.testing.PaimonTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

final class TestPaimonConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PaimonQueryRunner.builder()
                .setDataLoader(new PaimonTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_INSERT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
