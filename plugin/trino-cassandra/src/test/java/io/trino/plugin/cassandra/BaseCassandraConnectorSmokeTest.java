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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_DELETE_DATA;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseCassandraConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    public static final String KEYSPACE = "smoke_test";
    public static final ZonedDateTime TIMESTAMP_VALUE = ZonedDateTime.of(1970, 1, 1, 3, 4, 5, 0, ZoneId.of("UTC"));

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_MERGE,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
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

    @Test
    public void testInsertDate()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "(a_date date)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a_date) VALUES ( DATE '2020-05-11')", 1);
            assertThat(query("SELECT a_date FROM " + table.getName())).matches("VALUES (DATE '2020-05-11')");
        }
    }
}
