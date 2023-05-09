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
    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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
