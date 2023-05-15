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
package io.trino.execution;

import io.trino.Session;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.QueryUtil.table;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final QueryPreparer QUERY_PREPARER = new QueryPreparer(SQL_PARSER);

    @Test
    public void testSelectStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "SELECT * FROM foo");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteImmediateStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT * FROM foo'");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "execute my_query"))
                .hasErrorCode(NOT_FOUND);
    }

    @Test
    public void testExecuteImmediateInvalidStatement()
    {
        assertThatThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT FROM'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 1:27: mismatched input 'FROM'. Expecting: .*");
    }

    @Test
    public void testExecuteImmediateInvalidMultilineStatement()
    {
        assertThatThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE\nIMMEDIATE 'SELECT\n FROM'"))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("line 3:2: mismatched input 'FROM'. Expecting: .*");
    }

    @Test
    public void testTooManyParameters()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo where col1 = ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1,2"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT * FROM foo where col1 = ?' USING 1,2"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testTooFewParameters()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo where col1 = ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo where col1 = ?' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithOffset()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo OFFSET ? ROWS")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo OFFSET ? ROWS' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo OFFSET ? ROWS' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithLimit()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo LIMIT ?")
                .build();
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo LIMIT ?' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo LIMIT ?' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }

    @Test
    public void testParameterMismatchWithFetchFirst()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT ? FROM foo FETCH FIRST ? ROWS ONLY")
                .build();

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo FETCH FIRST ? ROWS ONLY' USING 1"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);

        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
        assertTrinoExceptionThrownBy(() -> QUERY_PREPARER.prepareQuery(TEST_SESSION, "EXECUTE IMMEDIATE 'SELECT ? FROM foo FETCH FIRST ? ROWS ONLY' USING 1, 2, 3, 4, 5, 6"))
                .hasErrorCode(INVALID_PARAMETER_USAGE);
    }
}
