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
package io.trino.plugin.redis;

import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseRedisConnectorTest
        extends BaseConnectorTest
{
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
                    SUPPORTS_DELETE,
                    SUPPORTS_INSERT,
                    SUPPORTS_MERGE,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testPredicatePushdown()
    {
        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19' AND regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // redis key equality
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key = 'tpch:nation:19' AND regionkey = 4"))
                .returnsEmptyResult()
                .isNotFullyPushedDown(FilterNode.class);

        // redis key different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE redis_key = 'tpch:nation:100'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // redis key IN case
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key IN ('tpch:nation:19', 'tpch:nation:2', 'tpch:nation:24')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '2', CAST('BRAZIL' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '24', CAST('UNITED STATES' AS varchar(25)))")
                .isFullyPushedDown();

        // redis key range
        assertThat(query("SELECT regionkey, nationkey, name FROM tpch.nation WHERE redis_key BETWEEN 'tpch:nation:23' AND 'tpch:nation:24'"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '23', CAST('UNITED KINGDOM' AS varchar(25))), " +
                        "(BIGINT '1', BIGINT '24', CAST('UNITED STATES' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);
    }
}
