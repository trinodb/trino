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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseJdbcCastPushdownTest
        extends AbstractTestQueryFramework
{
    protected abstract String leftTable();

    protected abstract String rightTable();

    protected abstract SqlExecutor onRemoteDatabase();

    protected abstract List<CastTestCase> supportedCastTypePushdown();

    protected abstract List<CastTestCase> unsupportedCastTypePushdown();

    protected abstract List<InvalidCastTestCase> invalidCast();

    @Test
    public void testProjectionPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), leftTable())))
                    .isFullyPushedDown();
        }

        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), leftTable())))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    public void testJoinPushdownWithCast()
    {
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .isFullyPushedDown();
        }

        for (CastTestCase testCase : unsupportedCastTypePushdown()) {
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .joinIsNotFullyPushedDown();
        }
    }

    @Test
    public void testInvalidCast()
    {
        for (InvalidCastTestCase testCase : invalidCast()) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), leftTable())))
                    .failure()
                    .hasMessageMatching(testCase.errorMessage());
        }
    }

    public record CastTestCase(String sourceColumn, String castType, String targetColumn)
    {
        public CastTestCase
        {
            requireNonNull(sourceColumn, "sourceColumn is null");
            requireNonNull(castType, "castType is null");
            requireNonNull(targetColumn, "targetColumn is null");
        }
    }

    public record InvalidCastTestCase(String sourceColumn, String castType, String errorMessage)
    {
        public InvalidCastTestCase(String sourceColumn, String castType)
        {
            this(sourceColumn, castType, "(.*)Cannot cast (.*) to (.*)");
        }

        public InvalidCastTestCase
        {
            requireNonNull(sourceColumn, "sourceColumn is null");
            requireNonNull(castType, "castType is null");
            requireNonNull(errorMessage, "errorMessage is null");
        }
    }
}
