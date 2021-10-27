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
package io.trino.plugin.redshift;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRedshiftClient
{
    private static final ConnectorSession MOCK_SESSION = new TestingConnectorSession.Builder().build();
    private static final JdbcClient JDBC_CLIENT = new RedshiftClient(
            new BaseJdbcConfig(),
            session -> { throw new UnsupportedOperationException(); },
            new DefaultIdentifierMapping());

    @Test
    public void testPushDownJoin()
    {
        JdbcColumnHandle leftColumn = new JdbcColumnHandle("a",
                new JdbcTypeHandle(Types.VARCHAR, Optional.of("VARCHAR"), Optional.of(50), Optional.empty(), Optional.empty(), Optional.of(CaseSensitivity.CASE_SENSITIVE)),
                VarcharType.VARCHAR);
        JdbcColumnHandle rightColumn = new JdbcColumnHandle("b",
                new JdbcTypeHandle(Types.VARCHAR, Optional.of("VARCHAR"), Optional.of(50), Optional.empty(), Optional.empty(), Optional.of(CaseSensitivity.CASE_SENSITIVE)),
                VarcharType.VARCHAR);
        Optional<PreparedQuery> preparedQuery = JDBC_CLIENT.implementJoin(MOCK_SESSION,
                JoinType.INNER,
                new PreparedQuery("table_a", List.of()),
                new PreparedQuery("table_b", List.of()),
                List.of(new JdbcJoinCondition(leftColumn, JoinCondition.Operator.EQUAL, rightColumn)),
                Map.of(rightColumn, "b"),
                Map.of(leftColumn, "a"),
                new JoinStatistics()
                {
                    @Override
                    public Optional<BasicRelationStatistics> getLeftStatistics()
                    {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<BasicRelationStatistics> getRightStatistics()
                    {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<BasicRelationStatistics> getJoinStatistics()
                    {
                        return Optional.empty();
                    }
                });

        assertTrue(preparedQuery.isPresent());
        String query = preparedQuery.get().getQuery();
        assertEquals(query, "SELECT l.\"a\" AS \"a\", r.\"b\" AS \"b\" FROM (table_a) l INNER JOIN (table_b) r ON l.\"a\" = r.\"b\"");
    }
}
