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
package io.prestosql.sql.query;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.assertions.PlanAssert;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

class QueryAssertions
        implements Closeable
{
    private final QueryRunner runner;

    public QueryAssertions()
    {
        this(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
    }

    public QueryAssertions(Session session)
    {
        this(LocalQueryRunner.create(session));
    }

    public QueryAssertions(QueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    public void assertFails(@Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        try {
            runner.execute(runner.getDefaultSession(), sql).toTestTypes();
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException exception) {
            exception.addSuppressed(new Exception("Query: " + sql));
            assertThat(exception)
                    .isInstanceOf(PrestoException.class)
                    .hasMessageMatching(expectedMessageRegExp);
        }
    }

    public void assertQueryAndPlan(
            @Language("SQL") String actual,
            @Language("SQL") String expected,
            PlanMatchPattern pattern)
    {
        assertQuery(actual, expected);

        Plan plan = runner.executeWithPlan(runner.getDefaultSession(), actual, WarningCollector.NOOP).getQueryPlan();
        PlanAssert.assertPlan(runner.getDefaultSession(), runner.getMetadata(), runner.getStatsCalculator(), plan, pattern);
    }

    public void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQuery(runner.getDefaultSession(), actual, expected, false);
    }

    public void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQuery(session, actual, expected, false);
    }

    public void assertQueryOrdered(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertQuery(runner.getDefaultSession(), actual, expected, true);
    }

    public void assertQuery(Session session, @Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
    {
        MaterializedResult actualResults = null;
        try {
            actualResults = execute(session, actual);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }

        MaterializedResult expectedResults = null;
        try {
            expectedResults = execute(expected);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }

        assertEquals(actualResults.getTypes(), expectedResults.getTypes(), "Types mismatch for query: \n " + actual + "\n:");

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertEquals(actualRows, expectedRows, "For query: \n " + actual + "\n:");
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + actual);
        }
    }

    public void assertQueryReturnsEmptyResult(@Language("SQL") String actual)
    {
        MaterializedResult actualResults = null;
        try {
            actualResults = execute(actual);
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }
        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        assertEquals(actualRows.size(), 0);
    }

    public static void assertContains(MaterializedResult all, MaterializedResult expectedSubset)
    {
        for (MaterializedRow row : expectedSubset.getMaterializedRows()) {
            if (!all.getMaterializedRows().contains(row)) {
                fail(format("expected row missing: %s\nAll %s rows:\n    %s\nExpected subset %s rows:\n    %s\n",
                        row,
                        all.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(all, 100)),
                        expectedSubset.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(expectedSubset, 100))));
            }
        }
    }

    public MaterializedResult execute(@Language("SQL") String query)
    {
        return execute(runner.getDefaultSession(), query);
    }

    public MaterializedResult execute(Session session, @Language("SQL") String query)
    {
        MaterializedResult actualResults;
        actualResults = runner.execute(session, query).toTestTypes();
        return actualResults;
    }

    @Override
    public void close()
    {
        runner.close();
    }

    public QueryRunner getQueryRunner()
    {
        return runner;
    }

    protected void executeExclusively(Runnable executionBlock)
    {
        runner.getExclusiveLock().lock();
        try {
            executionBlock.run();
        }
        finally {
            runner.getExclusiveLock().unlock();
        }
    }
}
