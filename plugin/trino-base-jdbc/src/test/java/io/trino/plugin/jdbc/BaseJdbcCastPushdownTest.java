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

import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.jdbc.CastDataTypeTest.CastTestCase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseJdbcCastPushdownTest
        extends AbstractTestQueryFramework
{
    protected CastDataTypeTest left;
    protected CastDataTypeTest right;

    protected abstract SqlExecutor onRemoteDatabase();

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
        if (left != null) {
            left.close();
        }
        if (right != null) {
            right.close();
        }
    }

    @Test
    public void testProjectionPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), left.tableName())))
                    .isFullyPushedDown();
        }

        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), left.tableName())))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    public void testCastFails()
    {
        for (CastTestCase testCase : failCast()) {
            assertThatThrownBy(() -> getQueryRunner().execute("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), left.tableName())))
                    .hasMessageMatching("(.*)Cannot cast (.*) to (.*)");
        }
    }

    @Test
    public void testJoinPushdownWithCast()
    {
        // combination of supported types
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertJoinFullyPushedDown(left.tableName(), right.tableName(), "JOIN", "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        }

        // combination of unsupported types
        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertJoinNotFullyPushedDown(left.tableName(), right.tableName(), "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        }
    }

    protected void assertJoinFullyPushedDown(String leftTable, String rightTable, String joinType, String joinCondition)
    {
        assertJoin(leftTable, rightTable, joinType, joinCondition)
                .isFullyPushedDown();
    }

    protected void assertJoinNotFullyPushedDown(String leftTable, String rightTable, String joinCondition)
    {
        assertJoin(leftTable, rightTable, "JOIN", joinCondition)
                .joinIsNotFullyPushedDown();
    }

    protected QueryAssertions.QueryAssert assertJoin(String leftTable, String rightTable, String joinType, String joinCondition)
    {
        return assertThat(query("SELECT l.id FROM %s l %s %s r ON %s".formatted(leftTable, joinType, rightTable, joinCondition)));
    }
}
