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
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.trino.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_DELETE_DATA;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createTestTables;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCassandraLatestConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String KEYSPACE = "smoke_test";
    private static final ZonedDateTime TIMESTAMP_VALUE = ZonedDateTime.of(1970, 1, 1, 3, 4, 5, 0, ZoneId.of("UTC"));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new CassandraServer("3.11.10"));
        CassandraSession session = server.getSession();
        createTestTables(session, KEYSPACE, Timestamp.from(TIMESTAMP_VALUE.toInstant()));
        return createCassandraQueryRunner(server, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_DELETE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testDeleteAllDataFromTable()
    {
        assertThatThrownBy(super::testDeleteAllDataFromTable)
                .hasMessageContaining("Deleting without partition key is not supported");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        String keyspaceAndTable = format("%s.%s", KEYSPACE, TABLE_DELETE_DATA);
        assertQuery("SELECT COUNT(*) FROM " + keyspaceAndTable, "VALUES 15");

        String wherePrimaryKey = " WHERE partition_one=3 AND partition_two=3 AND clust_one='clust_one_3'";
        assertUpdate("DELETE FROM " + keyspaceAndTable + wherePrimaryKey);

        assertQuery("SELECT COUNT(*) FROM " + keyspaceAndTable, "VALUES 14");
    }
}
