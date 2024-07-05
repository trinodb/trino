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

import io.trino.Session;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.plugin.jdbc.CastDataTypeTest.CastTestCase;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseCastPushdownTest
{
    protected final Session session;
    protected final QueryRunner queryRunner;
    protected final SqlExecutor sqlExecutor;
    protected final QueryAssertions assertions;

    protected CastDataTypeTest table1;
    protected CastDataTypeTest table2;

    public BaseCastPushdownTest(Session session, QueryRunner queryRunner, SqlExecutor sqlExecutor)
    {
        this.session = requireNonNull(session, "session is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.assertions = new QueryAssertions(queryRunner);
    }

    protected abstract void setupTable();

    protected abstract List<CastTestCase> supportedCastTypePushdown();

    protected abstract List<CastTestCase> unsupportedCastTypePushdown();

    protected abstract List<CastTestCase> failCast();

    @BeforeAll
    public void setup()
    {
        setupTable();
    }

    @AfterAll
    public void cleanup()
    {
        dropTable(table1);
        dropTable(table2);
    }

    private void dropTable(CastDataTypeTest table)
    {
        if (table2 != null) {
            queryRunner.execute("DROP TABLE IF EXISTS " + table.tableName());
        }
    }

    @Test
    public void testProjectionPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(assertions.query(session, "SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table1.tableName())))
                    .isFullyPushedDown();
        }

        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertThat(assertions.query(session, "SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table1.tableName())))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    public void testCastFails()
    {
        for (CastTestCase testCase : failCast()) {
            assertThatThrownBy(() -> queryRunner.execute(session, "SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table1.tableName())))
                    .hasMessageMatching("(.*)Cannot cast (.*) to (.*)");
        }
    }

    @Test
    public void testJoinPushdownWithCast()
    {
        // combination of supported types
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertJoinFullyPushedDown(session, table1.tableName(), table2.tableName(), "JOIN", "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        }

        // combination of unsupported types
        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertJoinNotFullyPushedDown(session, table1.tableName(), table2.tableName(), "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        }
    }

    protected void assertJoinFullyPushedDown(Session session, String leftTable, String rightTable, String joinType, String joinCondition)
    {
        assertJoin(session, leftTable, rightTable, joinType, joinCondition)
                .isFullyPushedDown();
    }

    protected void assertJoinNotFullyPushedDown(Session session, String leftTable, String rightTable, String joinCondition)
    {
        assertJoin(session, leftTable, rightTable, "JOIN", joinCondition)
                .joinIsNotFullyPushedDown();
    }

    protected QueryAssertions.QueryAssert assertJoin(Session session, String leftTable, String rightTable, String joinType, String joinCondition)
    {
        return assertThat(assertions.query(session, "SELECT l.id FROM %s l %s %s r ON %s".formatted(leftTable, joinType, rightTable, joinCondition)));
    }
}
